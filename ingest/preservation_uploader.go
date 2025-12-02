package ingest

import (
	ctx "context"
	"fmt"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"github.com/minio/minio-go/v7"
)

// PreservationUploader copies files from S3 staging to preservation storage.
type PreservationUploader struct {
	Base
}

// NewPreservationUploader returns a new PerservationUploader which can
// all files from S3 staging to preservation storage.
func NewPreservationUploader(context *common.Context, workItemID int64, ingestObject *service.IngestObject) *PreservationUploader {
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
	preservationBuckets := uploader.Context.Config.PreservationBucketsFor(uploader.IngestObject.StorageOption)

	return func(ingestFile *service.IngestFile) (errors []*service.ProcessingError) {
		for _, preservationBucket := range preservationBuckets {
			if !ingestFile.NeedsSaveAt(preservationBucket.Provider, preservationBucket.Bucket) {
				reason := "file has already been uploaded"
				if !ingestFile.HasPreservableName() {
					reason = "file does not have preservable name"
				} else if !ingestFile.NeedsSave {
					reason = "NeedsSave is false (unmodified reingest)"
				}
				uploader.Context.Logger.Infof("Skipping: %s because %s to %s/%s as %s",
					ingestFile.Identifier(), reason, preservationBucket.Provider,
					preservationBucket.Bucket, ingestFile.UUID)
				continue
			}
			var processingError *service.ProcessingError

			// User S3 server-side copying only in US East 1, where our
			// receiving buckets are. Cross-region server-side copying
			// is too slow. https://trello.com/c/52YwknCr
			if preservationBucket.Provider == constants.StorageProviderAWS && preservationBucket.Region == constants.RegionAWSUSEast1 {
				processingError = uploader.CopyToPreservationServerSide(ingestFile, preservationBucket)
			} else {
				processingError = uploader.CopyToPreservation(ingestFile, preservationBucket)
			}
			if processingError != nil {
				errors = append(errors, processingError)
			} else {
				// Add a StorageRecord to this file. Set only the
				// properties that indicate we've uploaded it.
				// Additional StorageRecord properties will be set
				// later when we confirm the upload succeeded.
				storageRecord := &service.StorageRecord{
					Bucket:   preservationBucket.Bucket,
					Provider: preservationBucket.Provider,
					StoredAt: time.Now().UTC(),
					URL:      preservationBucket.URLFor(ingestFile.UUID),
				}
				uploader.Context.Logger.Infof("Copied %s to %s/%s as %s", ingestFile.Identifier(), preservationBucket.Provider, preservationBucket.Bucket, ingestFile.UUID)
				ingestFile.SetStorageRecord(storageRecord)
			}
		}
		return errors
	}
}

// CopyToPreservationServerSide copies an object from AWS staging to AWS preservation.
// Since staging bucket and upload target are both within AWS US East 1,
// we can use CopyObject to do a bucket-to-bucket copy.
//
// Avoid calling this directly. Call Run() instead. This is
// public so we can test it.
func (uploader *PreservationUploader) CopyToPreservationServerSide(ingestFile *service.IngestFile, preservationBucket *common.PreservationBucket) *service.ProcessingError {
	client, err := uploader.getS3Client(preservationBucket.Provider)
	if err != nil {
		uploader.Context.Logger.Error(err, ingestFile.Identifier())
		return uploader.Error(ingestFile.Identifier(), err, false)
	}
	// Comments at https://github.com/minio/minio-go/blob/44ba45c1aa02cff384a840fe35950b50978bf620/api-compose-object.go#L48-L56
	// suggest that CopyObject will copy all of the object's user metadata
	// automatically. We'll need to test specifically to ensure that's true.
	// If not, change the last param of NewDestinationInfo to valid
	// userMeta map[string]string, which can come from ingestFile.GetPutOptions()
	sourceOpts := minio.CopySrcOptions{
		Bucket: uploader.Context.Config.StagingBucket,
		Object: uploader.S3KeyFor(ingestFile),
	}
	destOpts := minio.CopyDestOptions{
		Bucket: preservationBucket.Bucket,
		Object: ingestFile.UUID,
	}

	// CopyObject handles objects only up to 5GB.
	if ingestFile.Size <= constants.MaxServerSideCopySize {
		uploader.Context.Logger.Infof("Copying %s from %s to %s as %s using CopyObject()", ingestFile.Identifier(), uploader.Context.Config.StagingBucket, preservationBucket.Bucket, ingestFile.UUID)
		_, err = client.CopyObject(ctx.Background(), destOpts, sourceOpts)
	} else {
		uploader.Context.Logger.Infof("Copying %s from %s to %s as %s using ComposeObject()", ingestFile.Identifier(), uploader.Context.Config.StagingBucket, preservationBucket.Bucket, ingestFile.UUID)
		// ComposeObject handles items up to 5TB in a multipart server-to-server put.
		_, err = client.ComposeObject(ctx.Background(), destOpts, sourceOpts)
	}

	if err != nil {
		uploader.Context.Logger.Infof("Error copying %s (%s) to %s/%s: %v", ingestFile.Identifier(), ingestFile.UUID, preservationBucket.Provider, preservationBucket.Bucket, err)
		return uploader.Error(ingestFile.Identifier(), err, false)
	}
	return nil
}

// CopyToPreservation copies an object from AWS staging to an
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
func (uploader *PreservationUploader) CopyToPreservation(ingestFile *service.IngestFile, preservationBucket *common.PreservationBucket) *service.ProcessingError {
	srcClient, err := uploader.getS3Client(constants.StorageProviderAWS)
	if err != nil {
		uploader.Context.Logger.Error(ingestFile.Identifier(), err)
		return uploader.Error(ingestFile.Identifier(), err, false)
	}
	// Note that, while we normally just get a general client for the S3 provider,
	// Minio gets confused about which regions buckets are in. So in this case,
	// we get a specific client for the target bucket, with the region explicitly
	// pre-set. See https://trello.com/c/1yExAPkV
	destClient, err := uploader.getS3Client(preservationBucket.Bucket)
	if err != nil {
		uploader.Context.Logger.Error(ingestFile.Identifier(), err)
		return uploader.Error(ingestFile.Identifier(), err, false)
	}
	srcObject, err := srcClient.GetObject(
		ctx.Background(),
		uploader.Context.Config.StagingBucket,
		uploader.S3KeyFor(ingestFile),
		minio.GetObjectOptions{},
	)
	if err != nil {
		uploader.Context.Logger.Infof("Error getting source object for %s (%s/%s): %v", ingestFile.Identifier(), preservationBucket.Provider, preservationBucket.Bucket, err)
		return uploader.Error(ingestFile.Identifier(), err, false)
	}
	defer srcObject.Close()
	putOptions, err := ingestFile.GetPutOptions()
	if err != nil {
		uploader.Context.Logger.Infof("Error getting PutOptions for %s (%s/%s): %v", ingestFile.Identifier(), preservationBucket.Provider, preservationBucket.Bucket, err)
		return uploader.Error(ingestFile.Identifier(), err, false)
	}

	// Work-around for Wasabi multispace header bug. https://trello.com/c/SDasvwk8
	// For Wasabi, or a case where a narrow non-breaking space is included in the file path, use bagpath-encoded header. For all others, use bagpath.
	// Note that UserMetadata initially contains both.
	if util.StringListContains(constants.WasabiStorageProviders, preservationBucket.Provider) {
		delete(putOptions.UserMetadata, "bagpath") // or else Wasabi rejects this
		uploader.Context.Logger.Infof("For Wasabi, using header 'bagpath-encoded' with value %s", putOptions.UserMetadata["bagpath-encoded"])
	} else if strings.Contains(ingestFile.PathInBag, constants.NarrowNonBreakingSpace) || strings.Contains(ingestFile.PathInBag, constants.LineSeparator) {
		delete(putOptions.UserMetadata, "bagpath")
		uploader.Context.Logger.Infof("A narrow non-breaking space character was detected, using header 'bagpath-encoded' with value %s", putOptions.UserMetadata["bagpath-encoded"])
	} else {
		delete(putOptions.UserMetadata, "bagpath-encoded") // not necessary for other cases
	}

	uploader.Context.Logger.Infof("Copying %s (%s) from %s to %s using PutObject()", ingestFile.Identifier(), ingestFile.UUID, uploader.Context.Config.StagingBucket, preservationBucket.Bucket)

	uploadInfo, err := destClient.PutObject(
		ctx.Background(),
		preservationBucket.Bucket,
		ingestFile.UUID,
		srcObject,
		ingestFile.Size,
		putOptions,
	)
	if err != nil {
		uploader.Context.Logger.Infof("Error copying %s (%s) to %s/%s: %v", ingestFile.Identifier(), ingestFile.UUID, preservationBucket.Provider, preservationBucket.Bucket, err)
		return uploader.Error(ingestFile.Identifier(), err, false)
	}
	if uploadInfo.Size != ingestFile.Size {
		err = fmt.Errorf("Copied only %d of %d bytes from staging to preservation (UUID %s)", uploadInfo.Size, ingestFile.Size, ingestFile.UUID)
		return uploader.Error(ingestFile.Identifier(), err, false)
	}
	return nil
}

func (uploader *PreservationUploader) getS3Client(providerOrBucket string) (*minio.Client, error) {
	client := uploader.Context.S3Clients[providerOrBucket]
	if client == nil {
		return nil, fmt.Errorf("Cannot find S3 client for provider or bucket %s", providerOrBucket)
	}
	return client, nil
}
