package ingest

import (
	"fmt"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/minio/minio-go/v6"
)

type PreservationUploader struct {
	Worker
}

func NewPreservationUploader(context *common.Context, workItemID int, ingestObject *service.IngestObject) *PreservationUploader {
	return &PreservationUploader{
		Worker: Worker{
			Context:      context,
			IngestObject: ingestObject,
			WorkItemID:   workItemID,
		},
	}
}

// UploadAll uploads all of a bag's files that should be preserved to each
// of the preservation buckets in which they should be preserved. It returns
// the number of items preserved, and an error, if there was one.
//
// Note that "number of files preserved" will almost never match the number
// of files in the bag because:
//
// 1. We preserve all payload files and tag files, but we do not preserve
// bagit.txt, fetch.txt (which we consider illegal anyway), or any payload
// or tag manifests.
//
// 2. For certain preservation options, such as "Standard", we copy to two
// buckets in two different regions. That's two copies for each file. So a
// bag with 10 payload files can return a count of 22 if the storage option is
// "Standard" (10 * 2 payload files, plus 2 copies of aptrust-info.txt).
//
// The StorageRecords attached to each IngestFile record where and when each
// file was uploaded.
func (uploader *PreservationUploader) UploadAll() (int, error) {
	uploadFn := uploader.getUploadFunction()
	return uploader.Context.RedisClient.IngestFilesApply(uploader.WorkItemID, uploadFn)
}

func (uploader *PreservationUploader) getUploadFunction() func(*service.IngestFile) error {
	uploadTargets := uploader.Context.Config.UploadTargetsFor(uploader.IngestObject.StorageOption)

	return func(ingestFile *service.IngestFile) error {
		errMessages := make([]string, 0)
		for _, uploadTarget := range uploadTargets {
			if !ingestFile.NeedsSaveAt(uploadTarget.Provider, uploadTarget.Bucket) {
				continue
			}
			var err error
			if uploadTarget.Provider == constants.StorageProviderAWS {
				err = uploader.copyToAWSPreservation(ingestFile, uploadTarget)
			} else {
				err = uploader.copyToExternalPreservation(ingestFile, uploadTarget)
			}
			if err != nil {
				errMessages = append(errMessages, err.Error())
			} else {
				// Add a StorageRecord to this file. Set only the
				// properties that indicate we've uploaded it.
				// Additional StorageRecord properties will be set
				// later when we confirm the upload succeeded.
				storageRecord := &service.StorageRecord{
					Bucket:   uploadTarget.Bucket,
					Provider: uploadTarget.Provider,
					StoredAt: time.Now().UTC(),
					URL:      uploadTarget.URLFor(ingestFile.UUID),
				}
				ingestFile.SetStorageRecord(storageRecord)
			}
		}
		if len(errMessages) > 0 {
			return fmt.Errorf(strings.Join(errMessages, "; "))
		}
		return nil
	}
}

// Since staging bucket and upload target are both within AWS,
// we can use CopyObject to do a bucket-to-bucket copy.
func (uploader *PreservationUploader) copyToAWSPreservation(ingestFile *service.IngestFile, uploadTarget *common.UploadTarget) error {
	client, err := uploader.getS3Client(uploadTarget.Provider)
	if err != nil {
		return err
	}
	// Comments at https://github.com/minio/minio-go/blob/44ba45c1aa02cff384a840fe35950b50978bf620/api-compose-object.go#L48-L56
	// suggest that CopyObject will copy all of the object's user metadata
	// automatically. We'll need to test specifically to ensure that's true.
	// If not, change the last param of NewDestinationInfo to valid
	// userMeta map[string]string, which can come from ingestFile.GetPutOptions()
	sourceInfo := minio.NewSourceInfo(
		uploader.Context.Config.StagingBucket,
		uploader.S3KeyFor(ingestFile),
		nil,
	)
	destInfo, err := minio.NewDestinationInfo(
		uploadTarget.Bucket,
		ingestFile.UUID,
		nil,
		nil,
	)
	if err != nil {
		return fmt.Errorf("Error creating DestinationInfo: %s", err.Error())
	}
	return client.CopyObject(destInfo, sourceInfo)
}

// When copying from AWS staging to an external provider, we need two
// Minio clients: one that has credentials to connect to the source,
// and one with credentials to connect to the destination. We need to
// stream data from source, through localhost, to destination. That
// will be slow.
func (uploader *PreservationUploader) copyToExternalPreservation(ingestFile *service.IngestFile, uploadTarget *common.UploadTarget) error {
	srcClient, err := uploader.getS3Client(constants.StorageProviderAWS)
	if err != nil {
		return err
	}
	destClient, err := uploader.getS3Client(uploadTarget.Provider)
	if err != nil {
		return err
	}
	srcObject, err := srcClient.GetObject(
		uploader.Context.Config.StagingBucket,
		ingestFile.UUID,
		minio.GetObjectOptions{},
	)
	if err != nil {
		return fmt.Errorf("Error getting source object for copy: %s", err.Error())
	}
	putOptions, err := ingestFile.GetPutOptions()
	if err != nil {
		return err
	}
	bytesCopied, err := destClient.PutObject(
		uploadTarget.Bucket,
		ingestFile.UUID,
		srcObject,
		ingestFile.Size,
		putOptions,
	)
	if err != nil {
		return fmt.Errorf("Error copying object to preservation: %s", err.Error())
	}
	if bytesCopied != ingestFile.Size {
		return fmt.Errorf("Copied only %d of %d bytes from staging to preservation", bytesCopied, ingestFile.Size)
	}
	return nil
}

func (uploader *PreservationUploader) getS3Client(provider string) (*minio.Client, error) {
	client := uploader.Context.S3Clients[provider]
	if client == nil {
		return nil, fmt.Errorf("Cannot find S3 client for provider %s", provider)
	}
	return client, nil
}
