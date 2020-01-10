package ingest

import (
	"fmt"
	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"github.com/minio/minio-go/v6"
	"github.com/satori/go.uuid"
	"io"
	"os"
	"path/filepath"
	"time"
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
	Context      *common.Context
	IngestObject *service.IngestObject
	WorkItemId   int
}

// NewMetadataGatherer creates a new MetadataGatherer.
// The context parameter provides methods for communicating
// with S3 and our working data store (Redis).
func NewMetadataGatherer(context *common.Context, workItemId int, ingestObject *service.IngestObject) *MetadataGatherer {
	return &MetadataGatherer{
		Context:      context,
		IngestObject: ingestObject,
		WorkItemId:   workItemId,
	}
}

// ScanBag scans a tarred bag for metadata. This function can take
// less than a second or more than 24 hours to run, depending on the
// size of the bag we're scanning. (100kb takes less than a second,
// while multi-TB bags take more than 24 hours.) While it runs, it saves
// one IngestFile record at a time to the working data store.
//
// After scanning all files, it copies a handful of text files to our
// S3 staging bucket. The text files include manifests, tag manifests,
// and selected tag files.
func (m *MetadataGatherer) ScanBag() error {
	s3Obj, err := m.GetS3Object()
	if err != nil {
		return err
	}
	err = m.Context.RedisClient.IngestObjectSave(m.WorkItemId, m.IngestObject)
	if err != nil {
		return err
	}
	scanner := NewTarredBagScanner(
		s3Obj,
		m.IngestObject,
		m.Context.Config.IngestTempDir)
	defer scanner.Finish()
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
		err = m.Context.RedisClient.IngestFileSave(m.WorkItemId, ingestFile)
		if err != nil {
			m.logIngestFileNotSaved(ingestFile, err)
			return err
		}
		m.logIngestFileSaved(ingestFile)
	}

	err = m.CopyTempFilesToS3(scanner.TempFiles)
	if err != nil {
		return err
	}

	err = m.parseTempFiles(scanner.TempFiles)
	if err != nil {
		return err
	}

	return m.Context.RedisClient.IngestObjectSave(m.WorkItemId, m.IngestObject)
}

// GetS3Object retrieves a tarred bag from a depositor's receiving bucket.
func (m *MetadataGatherer) GetS3Object() (*minio.Object, error) {
	return m.Context.S3Clients[constants.S3ClientAWS].GetObject(
		m.IngestObject.S3Bucket,
		m.IngestObject.S3Key,
		minio.GetObjectOptions{})
}

// CopyTempFilesToS3 copies payload manifests, tag manifests, bagit.txt,
// bag-info.txt, and aptrust-info.txt to a staging bucket. At a later phase
// of ingest, the validator will examine the tag files for required tags,
// and it will compare the file checksums in the working data store with
// the checksums in the manifests.
func (m *MetadataGatherer) CopyTempFilesToS3(tempFiles []string) error {
	bucket := m.Context.Config.StagingBucket
	for _, filePath := range tempFiles {
		// All the files we save are in the top-level directory:
		// manifests, tag manifests, bagit.txt, bag-info.txt, and aptrust-info.txt
		basename := filepath.Base(filePath)
		// s3Key will look like 425005/manifest-sha256.txt
		key := fmt.Sprintf("%d/%s", m.WorkItemId, basename)

		// TODO: Fatal vs. transient errors. Retries.
		_, err := m.Context.S3Clients[constants.S3ClientAWS].FPutObject(
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
	if util.LooksLikeTagManifest(basename) {
		obj.TagManifests = append(obj.TagManifests, basename)
	} else if util.LooksLikeManifest(basename) {
		obj.Manifests = append(obj.Manifests, basename)
	} else {
		obj.ParsableTagFiles = append(obj.ParsableTagFiles, basename)
	}
}

func (m *MetadataGatherer) parseManifest(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	alg, err := util.GetAlgFromManifestName(filepath.Base(filename))
	checksums, err := bagit.ParseManifest(file, alg)
	if err != nil {
		return err
	}

	return m.updateChecksums(checksums)
}

func (m *MetadataGatherer) updateChecksums(checksums []*bagit.Checksum) error {
	for _, checksum := range checksums {
		err := m.addManifestChecksum(checksum)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MetadataGatherer) addManifestChecksum(checksum *bagit.Checksum) error {
	ingestChecksum := &service.IngestChecksum{
		Algorithm: checksum.Algorithm,
		DateTime:  time.Now().UTC(),
		Digest:    checksum.Digest,
		Source:    constants.SourceManifest,
	}
	ingestFile, err := m.Context.RedisClient.IngestFileGet(m.WorkItemId,
		m.IngestObject.FileIdentifier(checksum.Path))
	if err != nil {
		return err
	}
	// File is in manifest, but not in bag
	if ingestFile == nil {
		ingestFile = m.newIngestFile(checksum.Path)
	}
	ingestFile.Checksums = append(ingestFile.Checksums, ingestChecksum)
	return m.Context.RedisClient.IngestFileSave(m.WorkItemId, ingestFile)
}

func (m *MetadataGatherer) newIngestFile(relFilePath string) *service.IngestFile {
	ingestFile := service.NewIngestFile(m.IngestObject.Identifier(), relFilePath)
	ingestFile.UUID = uuid.NewV4().String()
	return ingestFile
}

func (m *MetadataGatherer) parseTagFile(filename string) error {
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

// ------------ Logging ------------

func (m *MetadataGatherer) logFileSaved(filename string) {
	m.Context.Logger.Infof("Copied to staging: WorkItem %d, %s",
		m.WorkItemId, filename)
}

func (m *MetadataGatherer) logFileNotSaved(filename string, err error) {
	m.Context.Logger.Errorf(
		"Failed copy to staging: WorkItem %d, %s: %s",
		m.WorkItemId, filename, err.Error())
}

func (m *MetadataGatherer) logIngestFileSaved(ingestFile *service.IngestFile) {
	m.Context.Logger.Infof("Saved to redis: WorkItem %d, %s",
		m.WorkItemId, ingestFile.Identifier())
}

func (m *MetadataGatherer) logIngestFileNotSaved(ingestFile *service.IngestFile, err error) {
	m.Context.Logger.Errorf(
		"Faild save to redis: WorkItem %d, %s: %s",
		m.WorkItemId, ingestFile.Identifier(), err.Error())
}
