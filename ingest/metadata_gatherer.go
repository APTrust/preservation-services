package ingest

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"github.com/minio/minio-go/v6"
	uuid "github.com/satori/go.uuid"
)

// MetadataGatherer scans a tarred bag, collects metadata such as
// filenames and checksums, and stores that metadata in an external
// datastore (currently Redis) for other ingest workers. It also
// copies payload manifests and parsable tag files to an S3 staging
// bucket.
//
// The worker performing the initial phase of the ingest process uses
// this object to gather the metadata that subsequent workers will
// need to perform their jobs.
type MetadataGatherer struct {
	Base
}

// NewMetadataGatherer creates a new MetadataGatherer.
// The context parameter provides methods for communicating
// with S3 and our working data store (Redis).
func NewMetadataGatherer(context *common.Context, workItemID int, ingestObject *service.IngestObject) *MetadataGatherer {
	return &MetadataGatherer{
		Base{
			Context:      context,
			IngestObject: ingestObject,
			WorkItemID:   workItemID,
		},
	}
}

// Run scans a tarred bag for metadata. This function can take
// less than a second or more than 24 hours to run, depending on the
// size of the bag we're scanning. (100kb takes less than a second,
// while multi-TB bags take more than 24 hours.) While it runs, it saves
// one IngestFile record at a time to the working data store.
//
// After scanning all files, it copies a handful of text files to our
// S3 staging bucket. The text files include manifests, tag manifests,
// and selected tag files.
func (m *MetadataGatherer) Run() (fileCount int, errors []*service.ProcessingError) {
	tarredBag, err := m.Context.S3GetObject(
		constants.StorageProviderAWS,
		m.IngestObject.S3Bucket,
		m.IngestObject.S3Key,
	)
	if err != nil {
		isFatal := false
		if strings.Contains(err.Error(), "key does not exist") {
			isFatal = true
		}
		return 0, append(errors, m.Error(m.IngestObject.Identifier(), err, isFatal))
	}

	// TODO: constants.MimeTypeForExtension[".tar"] is "application/x-tar"
	// both seem to be used, so APTrust and BTR profiles should probably
	// include both.
	m.IngestObject.Serialization = "application/tar"
	defer tarredBag.Close()
	scanner := NewTarredBagScanner(
		tarredBag,
		m.IngestObject,
		m.Context.Config.IngestTempDir)
	defer scanner.Finish()

	err = m.scan(scanner)
	if err != nil {
		return 0, append(errors, m.Error(m.IngestObject.Identifier(), err, false))
	}

	// Special action for staging system, where re-deployments can leave
	// stale manifests in the staging.staging bucket. These cause bag validation
	// to fail because the stale manifests include entries for files that do not
	// exist in the new bag.
	if m.Context.Config.StagingBucket == "aptrust.staging.staging" {
		m.deleteStaleItemsFromStaging(m.WorkItemID)
	}

	err = m.CopyTempFilesToS3(scanner.TempFiles)
	if err != nil {
		return 0, append(errors, m.Error(m.IngestObject.Identifier(), err, false))
	}

	err = m.parseTempFiles(scanner.TempFiles)
	if err != nil {
		return 0, append(errors, m.Error(m.IngestObject.Identifier(), err, false))
	}

	m.setMissingDefaultTags()

	err = m.IngestObjectSave()
	if err != nil {
		return 0, append(errors, m.Error(m.IngestObject.Identifier(), err, false))
	}

	return m.IngestObject.FileCount, errors
}

func (m *MetadataGatherer) scan(scanner *TarredBagScanner) error {
	for {
		ingestFile, err := scanner.ProcessNextEntry()
		// EOF expected at end of file
		if err == io.EOF {
			break
		}
		// Any non-EOF error is a problem
		if err != nil {
			return err
		}
		// ProcessNextEntry returns nil for directories,
		// symlinks, and anything else that's not a file.
		// We can't store these non-objects in S3, so we
		// ignore them.
		if ingestFile == nil {
			continue
		}

		// Make a note of tag files and fetch.txt file
		// for validator.
		m.noteSpecialFileType(ingestFile)
		m.IngestObject.FileCount++

		err = m.IngestFileSave(ingestFile)
		if err != nil {
			return err
		}
	}
	return nil
}

// CopyTempFilesToS3 copies payload manifests, tag manifests, bagit.txt,
// bag-info.txt, and aptrust-info.txt to a staging bucket. At a later phase
// of ingest, the validator will examine the tag files for required tags,
// and it will compare the file checksums in the working data store with
// the checksums in the manifests.
//
// We also want to keep these manifest and metadata files around for forensic
// purposes. If ingest stalls or fails, we may be able to find forensics info
// in these files. For example, sometimes file names, which appear in the
// manifests, contain strange unicode characters that S3 doesn't like.
func (m *MetadataGatherer) CopyTempFilesToS3(tempFiles []string) error {
	bucket := m.Context.Config.StagingBucket
	for _, filePath := range tempFiles {
		// All the files we save are in the top-level directory:
		// manifests, tag manifests, bagit.txt, bag-info.txt, and aptrust-info.txt
		basename := filepath.Base(filePath)
		// s3Key will look like 425005/manifest-sha256.txt
		key := fmt.Sprintf("%d/%s", m.WorkItemID, basename)

		// TODO: Fatal vs. transient errors. Retries.
		_, err := m.Context.S3Clients[constants.StorageProviderAWS].FPutObject(
			bucket,
			key,
			filePath,
			minio.PutObjectOptions{})
		if err != nil {
			m.logFileNotSaved(basename, err)
			return err
		}
		m.logFileSaved(basename)
	}
	return nil
}

func (m *MetadataGatherer) noteSpecialFileType(ingestFile *service.IngestFile) {
	fileType := ingestFile.FileType()
	if fileType == constants.FileTypeTag {
		m.IngestObject.TagFiles = append(m.IngestObject.TagFiles, ingestFile.PathInBag)
	} else if fileType == constants.FileTypeFetchTxt {
		m.IngestObject.HasFetchTxt = true
	}
}

func (m *MetadataGatherer) parseTempFiles(tempFiles []string) error {
	var err error
	for _, filename := range tempFiles {
		basename := filepath.Base(filename)
		m.addMetafilePathToObject(filename)
		if util.LooksLikeManifest(basename) || util.LooksLikeTagManifest(basename) {
			err = m.parseManifest(filename)
		} else {
			err = m.parseTagFile(filename)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MetadataGatherer) addMetafilePathToObject(filename string) {
	obj := m.IngestObject
	basename := filepath.Base(filename)
	alg, err := util.AlgorithmFromManifestName(filepath.Base(filename))
	if err != nil {
		alg = "Unknown Algorithm"
	}
	if util.LooksLikeTagManifest(basename) {
		obj.TagManifests = append(obj.TagManifests, alg)
	} else if util.LooksLikeManifest(basename) {
		obj.Manifests = append(obj.Manifests, alg)
	} else {
		obj.ParsableTagFiles = append(obj.ParsableTagFiles, basename)
	}
}

func (m *MetadataGatherer) parseManifest(filename string) error {
	m.Context.Logger.Infof("Parsing manifest %s", filename)
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	basename := filepath.Base(filename)
	alg, err := util.AlgorithmFromManifestName(basename)
	if err != nil {
		return err
	}
	checksums, err := bagit.ParseManifest(file, alg)
	if err != nil {
		return err
	}
	sourceType := constants.SourceManifest
	if util.LooksLikeTagManifest(basename) {
		sourceType = constants.SourceTagManifest
	}
	return m.updateChecksums(checksums, sourceType)
}

func (m *MetadataGatherer) updateChecksums(checksums []*bagit.Checksum, sourceType string) error {
	for _, checksum := range checksums {
		err := m.addManifestChecksum(checksum, sourceType)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MetadataGatherer) addManifestChecksum(checksum *bagit.Checksum, sourceType string) error {
	ingestChecksum := &service.IngestChecksum{
		Algorithm: checksum.Algorithm,
		DateTime:  time.Now().UTC(),
		Digest:    checksum.Digest,
		Source:    sourceType,
	}
	// Retry this Redis call because with smaller bags (< 20 files), the record
	// was likely posted to redis in the last few milliseconds, and Redis
	// sporadically replies with nil in this case in testing.
	var err error
	var ingestFile *service.IngestFile
	for i := 0; i < 3; i++ {
		gfIdentifier := m.IngestObject.FileIdentifier(checksum.Path)
		ingestFile, err = m.IngestFileGet(gfIdentifier)
		if err == nil {
			// We got the record.
			break
		} else {
			// No record. Clear the error and retry.
			err = nil
			time.Sleep(m.Context.Config.RedisRetryMs)
		}
	}
	// If no record after three tries, that's a problem.
	if err != nil {
		return err
	}
	// File is in manifest, but not in bag
	if ingestFile == nil {
		ingestFile = m.newIngestFile(checksum.Path)
	}
	ingestFile.Checksums = append(ingestFile.Checksums, ingestChecksum)
	return m.IngestFileSave(ingestFile)
}

func (m *MetadataGatherer) newIngestFile(relFilePath string) *service.IngestFile {
	ingestFile := service.NewIngestFile(m.IngestObject.Identifier(), relFilePath)
	ingestFile.UUID = uuid.NewV4().String()
	return ingestFile
}

func (m *MetadataGatherer) parseTagFile(filename string) error {
	m.Context.Logger.Infof("Parsing tag file %s", filename)
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	basename := filepath.Base(filename)
	tags, err := bagit.ParseTagFile(file, basename)
	if err != nil {
		return err
	}
	m.IngestObject.Tags = append(m.IngestObject.Tags, tags...)
	return nil
}

// Applies only to APTrust. Many depositors use bagging workflows
// implemented prior to 2019, when we started offering multiple storage
// options. Those workflows do not add in the Storage-Option tag.
// We have announced and documented that if Storage-Option is unspecified,
// it defaults to "Standard". We have to force this tag into bags where
// it's missing so that the validator will approve them.
func (m *MetadataGatherer) setMissingDefaultTags() {
	if m.IngestObject.BagItProfileFormat() == constants.BagItProfileDefault {
		tag := m.IngestObject.GetTag("aptrust-info.txt", "Storage-Option")
		if tag == nil {
			tag = bagit.NewTag("aptrust-info.txt", "Storage-Option", "Standard")
			m.IngestObject.Tags = append(m.IngestObject.Tags, tag)
		}
	}
}

// Delete stale manifests from the staging bucket. This problem affects
// our staging environment. See https://trello.com/c/cE9rLSUH
func (m *MetadataGatherer) deleteStaleItemsFromStaging(workItemId int) {
	if m.Context.Config.StagingBucket != "aptrust.staging.staging" {
		return
	}
	m.deleteStaleStagingItem(fmt.Sprintf("%d/%s", workItemId, "bagit.txt"))
	m.deleteStaleStagingItem(fmt.Sprintf("%d/%s", workItemId, "bag-info.txt"))
	m.deleteStaleStagingItem(fmt.Sprintf("%d/%s", workItemId, "aptrust-info.txt"))
	for _, alg := range constants.SupportedManifestAlgorithms {
		manifest := fmt.Sprintf("manifest-%s.txt", alg)
		tagManifest := fmt.Sprintf("tag%s", alg)
		m.deleteStaleStagingItem(fmt.Sprintf("%d/%s", workItemId, manifest))
		m.deleteStaleStagingItem(fmt.Sprintf("%d/%s", workItemId, tagManifest))
	}
}

func (m *MetadataGatherer) deleteStaleStagingItem(key string) {
	if m.Context.Config.StagingBucket != "aptrust.staging.staging" {
		return
	}
	s3Client := m.Context.S3Clients[constants.StorageProviderAWS]
	m.Context.Logger.Infof("Deleting stale item %s staging", key)
	err := s3Client.RemoveObject(m.Context.Config.StagingBucket, key)
	if err != nil && err.Error() != "The specified key does not exist." {
		m.Context.Logger.Warningf("Error deleting stale item %s staging: %v", key, err)
	}
}

// ------------ Logging ------------

func (m *MetadataGatherer) logFileSaved(filename string) {
	m.Context.Logger.Infof("Copied to staging: WorkItem %d, %s",
		m.WorkItemID, filename)
}

func (m *MetadataGatherer) logFileNotSaved(filename string, err error) {
	m.Context.Logger.Errorf(
		"Failed copy to staging: WorkItem %d, %s: %s",
		m.WorkItemID, filename, err.Error())
}
