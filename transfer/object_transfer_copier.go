package transfer

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

// Based on ingest_preservation_uploader

// TODO: Use the StorageRecord in models/services

// ObjectTransferCopier copies files from one bucket to another.
type ObjectTransferCopier struct {
	Base
}

// NewObjectTransferCopier returns a new ObjectTransferCopier which can
// copy all files of an object from one bucket to another.
func NewObjectTransferCopier(context *common.Context, workItemID int64, transferObject *service.TransferObject) *ObjectTransferCopier {
	return &ObjectTransferCopier{
		Base: Base{
			Context:        context,
			TransferObject: transferObject,
			WorkItemID:     workItemID,
		},
	}
}

// Run copies all of an object's files to a new specified bucket.
// It returns the number of files processed and an error, if there was one.
//
// Note that "number of files processed" should match the number of files
// in the object. That doesn't mean all of those files were copied,
// because bagit.txt, manifests and other files are never copied to
// preservation.
//
// The StorageRecords attached to each TransferFile record where and when each
// file was uploaded.
func (copier *ObjectTransferCopier) Run() (int, []*service.ProcessingError) {
	copyFn := copier.getCopyFunction()
	options := service.TransferFileApplyOptions{
		MaxErrors:   30,
		MaxRetries:  4,
		RetryMs:     1000,
		SaveChanges: true,
		WorkItemID:  copier.WorkItemID,
	}
	return copier.Context.RedisClient.TransferFilesApply(copyFn, options)
}

func (copier *ObjectTransferCopier) getCopyFunction() service.TransferFileApplyFn {
	// If Object is locked by another work item - do not run
	// Add double validation like with deletes

	targetPreservationBuckets := copier.Context.Config.PreservationBucketsFor(copier.TransferObject.TargetStorageOption)
	sourcePreservationBuckets := copier.Context.Config.PreservationBucketsFor(copier.TransferObject.SourceStorageOption)

	return func(transferFile *service.TransferFile) (errors []*service.ProcessingError) {
		for _, preservationBucket := range targetPreservationBuckets {
			if !transferFile.NeedsSaveAt(preservationBucket.Provider, preservationBucket.Bucket) {
				reason := "file has already been uploaded or copied over in transfer"
				if !transferFile.HasPreservableName() {
					reason = "file does not have preservable name in transfer"
				} else if !transferFile.NeedsSave {
					reason = "NeedsSave is false (during transfer)"
				}
				copier.Context.Logger.Infof("Skipping: %s because %s to %s/%s as %s",
					transferFile.Identifier(), reason, preservationBucket.Provider,
					preservationBucket.Bucket, transferFile.UUID)
				continue
			}
			var processingError *service.ProcessingError

			// User S3 server-side copying only in US East 1, where our
			// receiving buckets are. Cross-region server-side copying
			// is too slow. https://trello.com/c/52YwknCr

			// TODO: Since we could be copying from anywhere with the transfer case,
			// we may only use this when the source and target buckets are in the same region
			if preservationBucket.Provider == constants.StorageProviderAWS && preservationBucket.Region == constants.RegionAWSUSEast1 {
				processingError = copier.CopyToTargetPreservationServerSide(transferFile, sourcePreservationBuckets[0], preservationBucket)
			} else {
				processingError = copier.CopyToTargetPreservation(transferFile, sourcePreservationBuckets[0], preservationBucket)
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
					URL:      preservationBucket.URLFor(transferFile.UUID),
				}
				copier.Context.Logger.Infof("Transfer copied %s to %s/%s as %s", transferFile.Identifier(), preservationBucket.Provider, preservationBucket.Bucket, transferFile.UUID)
				transferFile.SetStorageRecord(storageRecord)
			}
		}
		return errors
	}
}

// CopyToTargetPreservationServerSide copies an object from one preservation bucket to another.
// This uses CopyObject to do a bucket-to-bucket copy if the source and target buckets are
// in the same region.
//
// Avoid calling this directly. Call Run() instead. This is
// public so we can test it.
func (copier *ObjectTransferCopier) CopyToTargetPreservationServerSide(transferFile *service.TransferFile, sourcePreservationBucket *common.PreservationBucket, targetPreservationBucket *common.PreservationBucket) *service.ProcessingError {
	client, err := copier.getS3Client(targetPreservationBucket.Provider)
	if err != nil {
		copier.Context.Logger.Error(err, transferFile.Identifier())
		return copier.Error(transferFile.Identifier(), err, false)
	}
	// Comments at https://github.com/minio/minio-go/blob/44ba45c1aa02cff384a840fe35950b50978bf620/api-compose-object.go#L48-L56
	// suggest that CopyObject will copy all of the object's user metadata
	// automatically. We'll need to test specifically to ensure that's true.
	// If not, change the last param of NewDestinationInfo to valid
	// userMeta map[string]string, which can come from ingestFile.GetPutOptions()
	sourceOpts := minio.CopySrcOptions{
		Bucket: sourcePreservationBucket.Bucket,
		Object: copier.S3KeyFor(transferFile),
	}
	destOpts := minio.CopyDestOptions{
		Bucket: targetPreservationBucket.Bucket,
		Object: transferFile.UUID,
	}

	// CopyObject handles objects only up to 5GB.
	if transferFile.Size <= constants.MaxServerSideCopySize {
		copier.Context.Logger.Infof("Copying during transfer %s from %s to %s as %s using CopyObject()", transferFile.Identifier(), copier.Context.Config.StagingBucket, targetPreservationBucket.Bucket, transferFile.UUID)
		_, err = client.CopyObject(ctx.Background(), destOpts, sourceOpts)
	} else {
		copier.Context.Logger.Infof("Copying during transfer %s from %s to %s as %s using ComposeObject()", transferFile.Identifier(), copier.Context.Config.StagingBucket, targetPreservationBucket.Bucket, transferFile.UUID)
		// ComposeObject handles items up to 5TB in a multipart server-to-server put.
		_, err = client.ComposeObject(ctx.Background(), destOpts, sourceOpts)
	}

	if err != nil {
		copier.Context.Logger.Infof("Error copying during transfer %s (%s) to %s/%s: %v", transferFile.Identifier(), transferFile.UUID, targetPreservationBucket.Provider, targetPreservationBucket.Bucket, err)
		return copier.Error(transferFile.Identifier(), err, false)
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
func (copier *ObjectTransferCopier) CopyToTargetPreservation(transferFile *service.TransferFile, sourcePreservationBucket *common.PreservationBucket, targetPreservationBucket *common.PreservationBucket) *service.ProcessingError {
	// srcClient, err := copier.getS3Client(constants.StorageProviderAWS)
	srcClient, err := copier.getS3Client(sourcePreservationBucket.Bucket)
	if err != nil {
		copier.Context.Logger.Error(transferFile.Identifier(), err)
		return copier.Error(transferFile.Identifier(), err, false)
	}
	// Note that, while we normally just get a general client for the S3 provider,
	// Minio gets confused about which regions buckets are in. So in this case,
	// we get a specific client for the target bucket, with the region explicitly
	// pre-set. See https://trello.com/c/1yExAPkV
	destClient, err := copier.getS3Client(targetPreservationBucket.Bucket)
	if err != nil {
		copier.Context.Logger.Error(transferFile.Identifier(), err)
		return copier.Error(transferFile.Identifier(), err, false)
	}
	// Get the object data out of its current place in preservation.
	// Note: This will be different for Glacier and Deep Glacier.
	// We will need to kick off a Glacier Restore request and bring it into an S3 bucket
	// Can we restore it directly to the preservation bucket? - cannot do this as the copy only lasts limited time
	// We would need to restore it FIRST to S3 before we get to here.
	srcObject, err := srcClient.GetObject(
		ctx.Background(),
		sourcePreservationBucket.Bucket,
		copier.S3KeyFor(transferFile),
		minio.GetObjectOptions{},
	)
	if err != nil {
		copier.Context.Logger.Infof("Error getting transfer source object for %s (%s/%s): %v", transferFile.Identifier(), sourcePreservationBucket.Provider, sourcePreservationBucket.Bucket, err)
		return copier.Error(transferFile.Identifier(), err, false)
	}
	defer srcObject.Close()
	putOptions, err := transferFile.GetPutOptions()
	if err != nil {
		copier.Context.Logger.Infof("Error getting transfer PutOptions for %s (%s/%s): %v", transferFile.Identifier(), sourcePreservationBucket.Provider, sourcePreservationBucket.Bucket, err)
		return copier.Error(transferFile.Identifier(), err, false)
	}

	// Work-around for Wasabi multispace header bug. https://trello.com/c/SDasvwk8
	// For Wasabi, or a case where a whitespace is included in the file path, use bagpath-encoded header. For all others, use bagpath.
	// Note that UserMetadata initially contains both.
	if util.StringListContains(constants.WasabiStorageProviders, targetPreservationBucket.Provider) {
		delete(putOptions.UserMetadata, "bagpath") // or else Wasabi rejects this
		copier.Context.Logger.Infof("For Wasabi, using header 'bagpath-encoded' with value %s", putOptions.UserMetadata["bagpath-encoded"])
	} else if strings.Contains(transferFile.PathInBag, constants.NarrowNonBreakingSpace) || strings.ContainsRune(transferFile.PathInBag, constants.LineSeparator) {
		delete(putOptions.UserMetadata, "bagpath")
		copier.Context.Logger.Infof("A whitespace character was detected, using header 'bagpath-encoded' with value %s", putOptions.UserMetadata["bagpath-encoded"])
	} else {
		delete(putOptions.UserMetadata, "bagpath-encoded") // not necessary for other cases
	}

	copier.Context.Logger.Infof("Copying via transfer %s (%s) from %s to %s using PutObject()", transferFile.Identifier(), transferFile.UUID, sourcePreservationBucket.Bucket, targetPreservationBucket.Bucket)

	uploadInfo, err := destClient.PutObject(
		ctx.Background(),
		targetPreservationBucket.Bucket,
		transferFile.UUID,
		srcObject,
		transferFile.Size,
		putOptions,
	)
	if err != nil {
		copier.Context.Logger.Infof("Error copying during transfer %s (%s) to %s/%s: %v", transferFile.Identifier(), transferFile.UUID, targetPreservationBucket.Provider, targetPreservationBucket.Bucket, err)
		return copier.Error(transferFile.Identifier(), err, false)
	}
	if uploadInfo.Size != transferFile.Size {
		err = fmt.Errorf("Copied only %d of %d bytes from source to target during transfer (UUID %s)", uploadInfo.Size, transferFile.Size, transferFile.UUID)
		return copier.Error(transferFile.Identifier(), err, false)
	}
	return nil
}

func (copier *ObjectTransferCopier) getS3Client(providerOrBucket string) (*minio.Client, error) {
	client := copier.Context.S3Clients[providerOrBucket]
	if client == nil {
		return nil, fmt.Errorf("Cannot find S3 client for provider or bucket %s", providerOrBucket)
	}
	return client, nil
}
