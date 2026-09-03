package transfer

import (
	ctx "context"
	"fmt"
	"strings"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/minio/minio-go/v7"
)

// Based on ingest_cleanup

// Cleanup cleans up all temporary files and data after ingest. This
// includes S3 files in the staging bucket and processing records in
// Redis.
type ObjectTransferCleanup struct {
	Base
}

// NewCleanup creates a new Cleanup. This will panic
// if the prerequisites for running the format identifier script are
// not present.
func NewObjectTransferCleanup(context *common.Context, workItemID int64, transferObject *service.TransferObject) *ObjectTransferCleanup {
	return &ObjectTransferCleanup{
		Base: Base{
			Context:        context,
			TransferObject: transferObject,
			WorkItemID:     workItemID,
		},
	}
}

func (c *ObjectTransferCleanup) Run() (fileCount int, errors []*service.ProcessingError) {
	ready := false
	if ready {
		fileCount, errors = c.deleteFilesFromSourceIfReady()
	}
	if len(errors) == 0 {
		c.Context.Logger.Infof("Deleting WorkItem %d from Redis", c.WorkItemID)
		_, err := c.Context.RedisClient.WorkItemDelete(c.WorkItemID)
		if err != nil {
			errors = append(errors, c.Error(c.TransferObject.Identifier(), err, false))
		}
	}
	if len(errors) == 0 {
		if c.TransferObject.ShouldDeleteFromReceiving {
			err := c.deleteFromReceiving()
			if err != nil {
				errors = append(errors, c.Error(c.TransferObject.Identifier(), err, false))
			}
		} else {
			c.Context.Logger.Warning("WorkItem %d: Not deleting %s/%s from receiving bucket because ShouldDeleteFromReceiving = false", c.WorkItemID, c.TransferObject.S3Bucket, c.TransferObject.S3Key)
		}
	}
	return fileCount, errors
}

// !!! ONLY call this when you are SURE the entire transfer completed successfully!
func (c *ObjectTransferCleanup) deleteFilesFromSourceIfReady() (fileCount int, errors []*service.ProcessingError) {
	maxErrors := 10
	stagingBucket := c.Context.Config.StagingBucket
	if BucketUnsafeForDeletion(stagingBucket) {
		c.Context.Logger.Fatalf("Cleanup worker will not delete from bucket %s, only from staging", stagingBucket)
	}

	// All items in staging bucket have the key <WorkItemID>/<File Identifier>
	prefix := fmt.Sprintf("%d", c.WorkItemID)

	s3Client := c.Context.S3Clients[constants.StorageProviderAWS]

	doneCh := make(chan struct{})
	defer close(doneCh)

	c.Context.Logger.Infof("WorkItem %d: cleaning up items in bucket %s with prefix %s", c.WorkItemID, stagingBucket, prefix)
	for obj := range s3Client.ListObjects(
		ctx.Background(),
		stagingBucket,
		minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		}) {

		if obj.Err != nil {
			errors = append(errors, c.Error(obj.Key, obj.Err, false))
			c.Context.Logger.Warningf("Error listing item: %s/%s - %s", stagingBucket, prefix, obj.Err.Error())
			if len(errors) > maxErrors {
				c.Context.Logger.Errorf("deleteFilesFromSourceIfReady is quitting before completion because it hit max errors.")
				return fileCount, errors
			}
			continue
		}
		err := s3Client.RemoveObject(ctx.Background(), stagingBucket, obj.Key, minio.RemoveObjectOptions{})
		if err != nil {
			errors = append(errors, c.Error(obj.Key, obj.Err, false))
			c.Context.Logger.Warningf("Error deleting %s - %s", obj.Key, obj.Err.Error())
			if len(errors) > maxErrors {
				c.Context.Logger.Errorf("deleteFilesFromSourceIfReady is quitting before completion because it hit max errors.")
				return fileCount, errors
			}
		} else {
			c.Context.Logger.Infof("Deleted from %s: %s", stagingBucket, obj.Key)
		}
		fileCount++
	}
	return fileCount, errors
}

// This is not needed
func (c *ObjectTransferCleanup) deleteFromReceiving() error {
	if BucketUnsafeForDeletion(c.TransferObject.S3Bucket) {
		return fmt.Errorf("Can't delete %s from receiving because bucket %s doesn't look safe", c.TransferObject.S3Key, c.TransferObject.S3Bucket)
	}
	s3Client := c.Context.S3Clients[constants.StorageProviderAWS]
	return s3Client.RemoveObject(ctx.Background(), c.TransferObject.S3Bucket, c.TransferObject.S3Key, minio.RemoveObjectOptions{})
}

func BucketUnsafeForDeletion(bucket string) bool {
	return !strings.Contains(bucket, "staging") && !strings.Contains(bucket, "receiving")
}
