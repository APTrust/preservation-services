package ingest

import (
	"fmt"
	"strings"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

// Cleanup cleans up all temporary files and data after ingest. This
// includes S3 files in the staging bucket and processing records in
// Redis.
type Cleanup struct {
	Worker
}

// NewCleanup creates a new Cleanup. This will panic
// if the prerequisites for running the format identifier script are
// not present.
func NewCleanup(context *common.Context, workItemID int, ingestObject *service.IngestObject) *Cleanup {
	return &Cleanup{
		Worker: Worker{
			Context:      context,
			IngestObject: ingestObject,
			WorkItemID:   workItemID,
		},
	}
}

func (c *Cleanup) CleanAll() (fileCount int, errors []*service.ProcessingError) {
	fileCount, errors = c.deleteFilesFromStaging()
	if len(errors) == 0 {
		_, err := c.Context.RedisClient.WorkItemDelete(c.WorkItemID)
		if err != nil {
			errors = append(errors, c.Error(c.IngestObject.Identifier(), err, false))
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

	for obj := range s3Client.ListObjects(stagingBucket, prefix, true, doneCh) {
		if obj.Err != nil {
			identifier := c.IngestObject.Identifier()
			if obj.Key != "" {
				identifier = obj.Key
			}
			errors = append(errors, c.Error(identifier, obj.Err, false))
			c.Context.Logger.Info("Error deleting from staging: %s - %s", identifier, obj.Err.Error())
			if len(errors) > maxErrors {
				return fileCount, errors
			}
		}
		err := s3Client.RemoveObject(stagingBucket, obj.Key)
		if err != nil {
			errors = append(errors, c.Error(obj.Key, obj.Err, false))
			c.Context.Logger.Info("Error deleting from staging: %s - %s", obj.Key, obj.Err.Error())
			if len(errors) > maxErrors {
				return fileCount, errors
			}
		}
		c.Context.Logger.Info("Deleted from staging: %s", obj.Key)
		fileCount++
	}
	return fileCount, errors
}

func BucketUnsafeForDeletion(bucket string) bool {
	return strings.Contains(bucket, "preservation") || !strings.Contains(bucket, "staging")
}
