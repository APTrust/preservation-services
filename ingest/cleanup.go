package ingest

import (
	ctx "context"
	"fmt"
	"strings"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/minio/minio-go/v7"
)

// Cleanup cleans up all temporary files and data after ingest. This
// includes S3 files in the staging bucket and processing records in
// Redis.
type Cleanup struct {
	Base
}

// NewCleanup creates a new Cleanup. This will panic
// if the prerequisites for running the format identifier script are
// not present.
func NewCleanup(context *common.Context, workItemID int64, ingestObject *service.IngestObject) *Cleanup {
	return &Cleanup{
		Base: Base{
			Context:      context,
			IngestObject: ingestObject,
			WorkItemID:   workItemID,
		},
	}
}

func (c *Cleanup) Run() (fileCount int, errors []*service.ProcessingError) {
	fileCount, errors = c.deleteFilesFromStaging()
	if len(errors) == 0 {
		c.Context.Logger.Infof("Deleting WorkItem %d from Redis", c.WorkItemID)
		_, err := c.Context.RedisClient.WorkItemDelete(c.WorkItemID)
		if err != nil {
			errors = append(errors, c.Error(c.IngestObject.Identifier(), err, false))
		}
	}
	if len(errors) == 0 {
		if c.IngestObject.ShouldDeleteFromReceiving {
			err := c.deleteFromReceiving()
			if err != nil {
				errors = append(errors, c.Error(c.IngestObject.Identifier(), err, false))
			}
		} else {
			c.Context.Logger.Warning("WorkItem %d: Not deleting %s/%s from receiving bucket because ShouldDeleteFromReceiving = false", c.WorkItemID, c.IngestObject.S3Bucket, c.IngestObject.S3Key)
		}
	}
	return fileCount, errors
}

func (c *Cleanup) deleteFilesFromStaging() (fileCount int, errors []*service.ProcessingError) {
	maxErrors := 10
	stagingBucket := c.Context.Config.StagingBucket
	if BucketUnsafeForDeletion(stagingBucket) {
		c.Context.Logger.Fatalf("Cleanup worker will not delete from bucket %s, only from staging", stagingBucket)
	}

	// All items in staging bucket have the key <WorkItemID>/<File Identifier>
	prefix := fmt.Sprintf("%d/", c.WorkItemID)

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
		// Filter out empty keys and ones like "staging.edu/btr-bag".
		// How do these even get into the list?
		if !strings.HasPrefix(obj.Key, prefix) {
			continue
		}
		if obj.Err != nil {
			errors = append(errors, c.Error(obj.Key, obj.Err, false))
			c.Context.Logger.Infof("Error listing item: %s/%s - %s", stagingBucket, obj.Key, obj.Err.Error())
			if len(errors) > maxErrors {
				return fileCount, errors
			}
		}
		err := s3Client.RemoveObject(ctx.Background(), stagingBucket, obj.Key, minio.RemoveObjectOptions{})
		if err != nil {
			errors = append(errors, c.Error(obj.Key, obj.Err, false))
			c.Context.Logger.Infof("Error deleting %s/%s - %s", stagingBucket, obj.Key, obj.Err.Error())
			if len(errors) > maxErrors {
				return fileCount, errors
			}
		}
		c.Context.Logger.Infof("Deleted from staging: %s", obj.Key)
		fileCount++
	}
	return fileCount, errors
}

func (c *Cleanup) deleteFromReceiving() error {
	if BucketUnsafeForDeletion(c.IngestObject.S3Bucket) {
		return fmt.Errorf("Can't delete %s from receiving because bucket %s doesn't look safe", c.IngestObject.S3Key, c.IngestObject.S3Bucket)
	}
	s3Client := c.Context.S3Clients[constants.StorageProviderAWS]
	return s3Client.RemoveObject(ctx.Background(), c.IngestObject.S3Bucket, c.IngestObject.S3Key, minio.RemoveObjectOptions{})
}

func BucketUnsafeForDeletion(bucket string) bool {
	return !strings.Contains(bucket, "staging") && !strings.Contains(bucket, "receiving")
}
