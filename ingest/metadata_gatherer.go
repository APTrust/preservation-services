package ingest

import (
	"fmt"
	"github.com/APTrust/preservation-services/models"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/minio/minio-go/v6"
	"io"
	"path/filepath"
)

type MetadataGatherer struct {
	Context *models.Context
}

func NewMetaDataGatherer(context *models.Context) *MetadataGatherer {
	return &MetadataGatherer{
		Context: context,
	}
}

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
		err = m.Context.RedisClient.IngestFileSave(workItemId, ingestFile)
		if err != nil {
			return err
		}
	}
	m.CopyTempFilesToS3(workItemId, scanner.TempFiles)
	return nil
}

func (m *MetadataGatherer) GetS3Object(ingestObject *service.IngestObject) (*minio.Object, error) {
	return m.Context.S3Clients["AWS"].GetObject(
		ingestObject.S3Bucket,
		ingestObject.S3Key,
		minio.GetObjectOptions{})
}

func (m *MetadataGatherer) CopyTempFilesToS3(workItemId int, tempFiles []string) error {
	bucket := m.Context.Config.IngestStagingBucket
	for _, filePath := range tempFiles {
		// All the files we save are in the top-level directory:
		// manifests, tag manifests, bagit.txt, bag-info.txt, and aptrust-info.txt
		basename := filepath.Base(filePath)
		// s3Key will look like 425005/manifest-sha256.txt
		key := fmt.Sprintf("%d/%s", workItemId, basename)

		//m.Context.Logger.Info("Copying %s to %s/%s", filePath, bucket, key)

		// TODO: Fatal vs. transient errors. Retries.
		_, err := m.Context.S3Clients["AWS"].FPutObject(
			bucket,
			key,
			filePath,
			minio.PutObjectOptions{})

		if err != nil {
			return err
		}
	}
	return nil
}
