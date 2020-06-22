package ingest

import (
	"fmt"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/minio/minio-go/v6"
)

// PreservationUploader copies files from S3 staging to preservation storage.
type PreservationUploader struct {
	Base
}

// NewPreservationUploader returns a new PerservationUploader which can
// all files from S3 staging to preservation storage.
func NewPreservationUploader(context *common.Context, workItemID int, ingestObject *service.IngestObject) *PreservationUploader {
	return &PreservationUploader{
		Base: Base{
			Context:      context,
			IngestObject: ingestObject,
			WorkItemID:   workItemID,
		},
	}
}

// Run uploads all of a bag's files that should be preserved to each
// of the preservation buckets in which they should be preserved. It returns
// the number of files processed and an error, if there was one.
//
// Note that "number of files processed" should match the number of files
// in the bag. That doesn't mean all of those files were copied to preservation
// because bagit.txt, manifests and other files are never copied to
// preservation.
//
// The StorageRecords attached to each IngestFile record where and when each
// file was uploaded.
func (uploader *PreservationUploader) Run() (int, []*service.ProcessingError) {
	uploadFn := uploader.getUploadFunction()
	options := service.IngestFileApplyOptions{
		MaxErrors:   30,
		MaxRetries:  4,
		RetryMs:     1000,
		SaveChanges: true,
		WorkItemID:  uploader.WorkItemID,
	}
	return uploader.Context.RedisClient.IngestFilesApply(uploadFn, options)
}

func (uploader *PreservationUploader) getUploadFunction() service.IngestFileApplyFn {
	uploadTargets := uploader.Context.Config.UploadTargetsFor(uploader.IngestObject.StorageOption)

	return func(ingestFile *service.IngestFile) (errors []*service.ProcessingError) {
		for _, uploadTarget := range uploadTargets {
			if !ingestFile.NeedsSaveAt(uploadTarget.Provider, uploadTarget.Bucket) {
				uploader.Context.Logger.Infof("Skipping: %s already uploaded to %s/%s", ingestFile.Identifier(), uploadTarget.Provider, uploadTarget.Bucket)
				continue
			}
			uploader.Context.Logger.Infof("Copying %s to %s/%s", ingestFile.Identifier(), uploadTarget.Provider, uploadTarget.Bucket)
			var processingError *service.ProcessingError

			if uploadTarget.Provider == constants.StorageProviderAWS {
				processingError = uploader.CopyToAWSPreservation(ingestFile, uploadTarget)
			} else {
				processingError = uploader.CopyToExternalPreservation(ingestFile, uploadTarget)
			}
			if processingError != nil {
				errors = append(errors, processingError)
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
				uploader.Context.Logger.Infof("Copied %s to %s/%s", ingestFile.Identifier(), uploadTarget.Provider, uploadTarget.Bucket)
				ingestFile.SetStorageRecord(storageRecord)
			}
		}
		return errors
	}
}

// CopyToAWSPreservation copies an object from AWS staging to AWS preservation.
// Since staging bucket and upload target are both within AWS,
// we can use CopyObject to do a bucket-to-bucket copy.
//
// Avoid calling this directly. Call Run() instead. This is
// public so we can test it.
func (uploader *PreservationUploader) CopyToAWSPreservation(ingestFile *service.IngestFile, uploadTarget *common.UploadTarget) *service.ProcessingError {
	client, err := uploader.getS3Client(uploadTarget.Provider)
	if err != nil {
		uploader.Context.Logger.Error(err, ingestFile.Identifier())
		return uploader.Error(ingestFile.Identifier(), err, false)
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
		uploader.Context.Logger.Infof("Error getting destination info for %s (%s/%s): %v", ingestFile.Identifier(), uploadTarget.Provider, uploadTarget.Bucket, err)
		return uploader.Error(ingestFile.Identifier(), err, false)
	}
	uploader.Context.Logger.Infof("Copying %s from %s to %s using CopyObject()", ingestFile.Identifier(), uploader.Context.Config.StagingBucket, uploadTarget.Bucket)

	// CopyObject handles objects only up to 5GB.
	//err = client.CopyObject(destInfo, sourceInfo)

	// ComposeObject *should* handle objects of any size.
	// Initial Minio bug report: https://github.com/minio/minio-go/pull/644
	// Migrated to: https://github.com/minio/minio-go/pull/715
	sources := []minio.SourceInfo{
		sourceInfo,
	}
	err = client.ComposeObject(destInfo, sources)

	if err != nil {
		uploader.Context.Logger.Infof("Error copying %s to %s/%s: %v", ingestFile.Identifier(), uploadTarget.Provider, uploadTarget.Bucket, err)
		return uploader.Error(ingestFile.Identifier(), err, false)
	}
	return nil
}

// CopyToExternalPreservation copies an object from AWS staging to an
// external S3 provider, like Wasabi.
//
// When copying from AWS staging to an external provider, we need two
// Minio clients: one that has credentials to connect to the source,
// and one with credentials to connect to the destination. We need to
// stream data from source, through localhost, to destination. That
// will be slow.
//
// Avoid calling this directly. Call Run() instead. This is
// public so we can test it.
func (uploader *PreservationUploader) CopyToExternalPreservation(ingestFile *service.IngestFile, uploadTarget *common.UploadTarget) *service.ProcessingError {
	srcClient, err := uploader.getS3Client(constants.StorageProviderAWS)
	if err != nil {
		uploader.Context.Logger.Error(ingestFile.Identifier(), err)
		return uploader.Error(ingestFile.Identifier(), err, false)
	}
	destClient, err := uploader.getS3Client(uploadTarget.Provider)
	if err != nil {
		uploader.Context.Logger.Error(ingestFile.Identifier(), err)
		return uploader.Error(ingestFile.Identifier(), err, false)
	}
	srcObject, err := srcClient.GetObject(
		uploader.Context.Config.StagingBucket,
		uploader.S3KeyFor(ingestFile),
		minio.GetObjectOptions{},
	)
	if err != nil {
		uploader.Context.Logger.Infof("Error getting source object for %s (%s/%s): %v", ingestFile.Identifier(), uploadTarget.Provider, uploadTarget.Bucket, err)
		return uploader.Error(ingestFile.Identifier(), err, false)
	}
	putOptions, err := ingestFile.GetPutOptions()
	if err != nil {
		uploader.Context.Logger.Infof("Error getting PutOptions for %s (%s/%s): %v", ingestFile.Identifier(), uploadTarget.Provider, uploadTarget.Bucket, err)
		return uploader.Error(ingestFile.Identifier(), err, false)
	}

	uploader.Context.Logger.Infof("Copying %s from %s to %s using PutObject()", ingestFile.Identifier(), uploader.Context.Config.StagingBucket, uploadTarget.Bucket)

	bytesCopied, err := destClient.PutObject(
		uploadTarget.Bucket,
		ingestFile.UUID,
		srcObject,
		ingestFile.Size,
		putOptions,
	)
	if err != nil {
		uploader.Context.Logger.Infof("Error copying %s to %s/%s: %v", ingestFile.Identifier(), uploadTarget.Provider, uploadTarget.Bucket, err)
		return uploader.Error(ingestFile.Identifier(), err, false)
	}
	if bytesCopied != ingestFile.Size {
		err = fmt.Errorf("Copied only %d of %d bytes from staging to preservation", bytesCopied, ingestFile.Size)
		return uploader.Error(ingestFile.Identifier(), err, false)
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
