package ingest

import (
	"archive/tar"
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"github.com/minio/minio-go/v6"
	"io"
	"time"
)

// StagingUploader unpacks a tarfile from a receiving bucket and
// stores each file unpacked from the tar in a staging bucket.
type StagingUploader struct {
	Context      *common.Context
	IngestObject *service.IngestObject
	WorkItemId   int
}

// NewStagingUploader creates a new StagingUploader.
func NewStagingUploader(context *common.Context, workItemId int, ingestObject *service.IngestObject) *StagingUploader {
	return &StagingUploader{
		Context:      context,
		IngestObject: ingestObject,
		WorkItemId:   workItemId,
	}
}

func (s *StagingUploader) CopyFilesToStaging() error {
	tarredBag, err := s.GetS3Object()
	if err != nil {
		return err
	}
	defer tarredBag.Close()
	err = s.CopyFiles(tarredBag)
	if err != nil {
		return err
	}
	s.IngestObject.CopiedToStagingAt = time.Now().UTC()
	return s.Context.RedisClient.IngestObjectSave(s.WorkItemId, s.IngestObject)
}

func (s *StagingUploader) GetS3Object() (*minio.Object, error) {
	return s.Context.S3Clients[constants.S3ClientAWS].GetObject(
		s.IngestObject.S3Bucket,
		s.IngestObject.S3Key,
		minio.GetObjectOptions{})
}

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
				if err == nil {
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

func (s *StagingUploader) CopyFileToStaging(tarReader *tar.Reader, ingestFile *service.IngestFile) error {
	putOptions, err := s.GetPutOptions(ingestFile)
	if err != nil {
		// TODO: This is a fatal error. Need to mark as such & stop processing.
		return err
	}
	bucket := s.Context.Config.StagingBucket
	key := fmt.Sprintf("%d/%s", s.WorkItemId, ingestFile.UUID)
	_, err = s.Context.S3Clients[constants.S3ClientAWS].PutObject(
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

func (s *StagingUploader) GetPutOptions(ingestFile *service.IngestFile) (minio.PutObjectOptions, error) {
	emptyOpts := minio.PutObjectOptions{}
	md5 := ingestFile.GetChecksum(constants.SourceIngest, constants.AlgMd5)
	if md5 == nil {
		return emptyOpts, fmt.Errorf("%s has no ingest md5 checksum", ingestFile.Identifier())
	}
	sha256 := ingestFile.GetChecksum(constants.SourceIngest, constants.AlgSha256)
	if sha256 == nil {
		return emptyOpts, fmt.Errorf("%s has no ingest sha256 checksum", ingestFile.Identifier())
	}
	return minio.PutObjectOptions{
		UserMetadata: map[string]string{
			"institution": s.IngestObject.Institution,
			"bag":         s.IngestObject.Identifier(),
			"bagpath":     ingestFile.PathInBag,
			"md5":         md5.Digest,
			"sha256":      sha256.Digest,
		},
		ContentType: ingestFile.FileFormat,
	}, nil
}

func (s *StagingUploader) MarkFileAsCopied(ingestFile *service.IngestFile) error {
	ingestFile.CopiedToStagingAt = time.Now().UTC()
	err := s.Context.RedisClient.IngestFileSave(s.WorkItemId, ingestFile)
	if err != nil {
		return fmt.Errorf("%s was copied to staging but could not be updated in Redis: %v", ingestFile.Identifier(), err)
	}
	return nil
}

func (s *StagingUploader) GetIngestFile(name string) (*service.IngestFile, error) {
	identifier, err := s.GetGenericFileIdentifier(name)
	if err != nil {
		return nil, err
	}
	return s.Context.RedisClient.IngestFileGet(s.WorkItemId, identifier)
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
