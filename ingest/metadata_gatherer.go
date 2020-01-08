package ingest

import (
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/minio/minio-go/v6"
	"io"
	"path/filepath"
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
	Context *common.Context
}

// NewMetadataGatherer creates a new MetadataGatherer.
// The context parameter provides methods for communicating
// with S3 and our working data store (Redis).
func NewMetadataGatherer(context *common.Context) *MetadataGatherer {
	return &MetadataGatherer{
		Context: context,
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
func (m *MetadataGatherer) ScanBag(workItemId int, ingestObject *service.IngestObject) error {
	s3Obj, err := m.GetS3Object(ingestObject)
	if err != nil {
		return err
	}
	scanner := NewTarredBagScanner(
		s3Obj,
		ingestObject,
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
		err = m.Context.RedisClient.IngestFileSave(workItemId, ingestFile)
		if err != nil {
			m.logIngestFileNotSaved(workItemId, ingestFile, err)
			return err
		}
		m.logIngestFileSaved(workItemId, ingestFile)
	}
	m.CopyTempFilesToS3(workItemId, scanner.TempFiles)
	return nil
}

// GetS3Object retrieves a tarred bag from a depositor's receiving bucket.
func (m *MetadataGatherer) GetS3Object(ingestObject *service.IngestObject) (*minio.Object, error) {
	return m.Context.S3Clients[constants.S3ClientAWS].GetObject(
		ingestObject.S3Bucket,
		ingestObject.S3Key,
		minio.GetObjectOptions{})
}

// CopyTempFilesToS3 copies payload manifests, tag manifests, bagit.txt,
// bag-info.txt, and aptrust-info.txt to a staging bucket. At a later phase
// of ingest, the validator will examine the tag files for required tags,
// and it will compare the file checksums in the working data store with
// the checksums in the manifests.
func (m *MetadataGatherer) CopyTempFilesToS3(workItemId int, tempFiles []string) error {
	bucket := m.Context.Config.StagingBucket
	for _, filePath := range tempFiles {
		// All the files we save are in the top-level directory:
		// manifests, tag manifests, bagit.txt, bag-info.txt, and aptrust-info.txt
		basename := filepath.Base(filePath)
		// s3Key will look like 425005/manifest-sha256.txt
		key := fmt.Sprintf("%d/%s", workItemId, basename)

		//m.Context.Logger.Info("Copying %s to %s/%s", filePath, bucket, key)

		// TODO: Fatal vs. transient errors. Retries.
		_, err := m.Context.S3Clients[constants.S3ClientAWS].FPutObject(
			bucket,
			key,
			filePath,
			minio.PutObjectOptions{})
		if err != nil {
			m.logFileNotSaved(workItemId, basename, err)
			return err
		}
		m.logFileSaved(workItemId, basename)
	}
	return nil
}

// ------------ Logging ------------

func (m *MetadataGatherer) logFileSaved(workItemId int, filename string) {
	m.Context.Logger.Infof("Copied to staging: WorkItem %d, %s",
		workItemId, filename)
}

func (m *MetadataGatherer) logFileNotSaved(workItemId int, filename string, err error) {
	m.Context.Logger.Errorf(
		"Failed copy to staging: WorkItem %d, %s: %s",
		workItemId, filename, err.Error())
}

func (m *MetadataGatherer) logIngestFileSaved(workItemId int, ingestFile *service.IngestFile) {
	m.Context.Logger.Infof("Saved to redis: WorkItem %d, %s",
		workItemId, ingestFile.Identifier())
}

func (m *MetadataGatherer) logIngestFileNotSaved(workItemId int, ingestFile *service.IngestFile, err error) {
	m.Context.Logger.Errorf(
		"Faild save to redis: WorkItem %d, %s: %s",
		workItemId, ingestFile.Identifier(), err.Error())
}
