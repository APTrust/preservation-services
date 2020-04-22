package ingest

import (
	"archive/tar"
	"fmt"
	"io"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"github.com/minio/minio-go/v6"
)

// StagingUploader unpacks a tarfile from a receiving bucket and
// stores each file unpacked from the tar in a staging bucket.
type StagingUploader struct {
	Base
}

// NewStagingUploader creates a new StagingUploader.
func NewStagingUploader(context *common.Context, workItemID int, ingestObject *service.IngestObject) *StagingUploader {
	return &StagingUploader{
		Base{
			Context:      context,
			IngestObject: ingestObject,
			WorkItemID:   workItemID,
		},
	}
}

// CopyFilesToStaging does all of the work, including:
//
// 1. Retrieving the tarred bag from the depositor's receiving bucket.
//
// 2. Copying the bag's individual files to a staging bucket with correct
//    metadata.
//
// 3. Telling Redis that each file has been copied.
//
// This is the only method external callers need to call.
func (s *StagingUploader) CopyFilesToStaging() error {
	tarredBag, err := s.Context.S3GetObject(
		constants.StorageProviderAWS,
		s.IngestObject.S3Bucket,
		s.IngestObject.S3Key,
	)
	if err != nil {
		return err
	}
	defer tarredBag.Close()
	err = s.CopyFiles(tarredBag)
	if err != nil {
		return err
	}
	s.IngestObject.CopiedToStagingAt = time.Now().UTC()
	return s.IngestObjectSave()
}

// CopyFiles unpacks files from a tarball copies each individual
// file to an S3 staging bucket so we can work with individual files
// later. There is no need to call this directly. Use CopyFilesToStaging()
// instead.
func (s *StagingUploader) CopyFiles(tarredBag *minio.Object) error {
	errCount := 0
	tarReader := tar.NewReader(tarredBag)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if header.Typeflag == tar.TypeReg {
			ingestFile, err := s.GetIngestFile(header.Name)
			if err != nil {
				return err
			}
			if ingestFile.CopiedToStagingAt.IsZero() {
				err := s.CopyFileToStaging(tarReader, ingestFile)
				if err != nil {
					// Most S3 copy errors are transient. Log this
					// as a warning, and we can retry later.
					s.Context.Logger.Warning(err.Error())
					errCount++
				}
			} else {
				s.Context.Logger.Infof("Copied %s to staging", ingestFile.Identifier())
			}
		}
	}
	if errCount > 0 {
		return fmt.Errorf("%d files were not copied", errCount)
	}
	return nil
}

// CopyFileToStaging copies a single file from the tarball to the staging
// bucket, and updates the IngestFile's Redis record to indicate it's been
// copied.
func (s *StagingUploader) CopyFileToStaging(tarReader *tar.Reader, ingestFile *service.IngestFile) error {
	putOptions, err := ingestFile.GetPutOptions()
	if err != nil {
		// TODO: This is a fatal error. Need to mark as such & stop processing.
		return err
	}
	bucket := s.Context.Config.StagingBucket
	key := s.S3KeyFor(ingestFile)
	_, err = s.Context.S3Clients[constants.StorageProviderAWS].PutObject(
		bucket,
		key,
		tarReader,
		ingestFile.Size,
		putOptions)
	if err != nil {
		return fmt.Errorf("Error copying %s to staging: %v", ingestFile.Identifier(), err)
	}
	return s.MarkFileAsCopied(ingestFile)
}

// MarkFileAsCopied adds a timestamp to the IngestFile record and saves the
// record to Redis, so we know when it was copied to staging.
func (s *StagingUploader) MarkFileAsCopied(ingestFile *service.IngestFile) error {
	ingestFile.CopiedToStagingAt = time.Now().UTC()
	return s.IngestFileSave(ingestFile)
}

// GetIngestFile returns the IngestFile record from Redis. The name param
// comes from the tar.Header.Name, and is translated interally into a
// GenericFileIdentifier by GetGenericFileIdentifier.
func (s *StagingUploader) GetIngestFile(name string) (*service.IngestFile, error) {
	identifier, err := s.GetGenericFileIdentifier(name)
	if err != nil {
		return nil, err
	}
	return s.IngestFileGet(identifier)
}

// GetGenericFileIdentifier converts the name from the tar header into
// the GenericFile identifier. The tar header name will typically look
// like "bagname/data/file.txt", while the GenericFile identifier should
// look like "test.edu/bagname/data/file.txt"
func (s *StagingUploader) GetGenericFileIdentifier(name string) (string, error) {
	pathInBag, err := util.TarPathToBagPath(name)
	if err != nil {
		return "", err
	}
	return s.IngestObject.FileIdentifier(pathInBag), nil
}
