// +build integration

package ingest_test

import (
	"fmt"
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const cleanupItemID = 44313

func TestNewCleanup(t *testing.T) {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	cleanup := ingest.NewCleanup(context, 333, obj)
	require.NotNil(t, cleanup)
	assert.Equal(t, context, cleanup.Context)
	assert.Equal(t, obj, cleanup.IngestObject)
	assert.Equal(t, 333, cleanup.WorkItemID)
}

func TestBucketUnsafeForDeletion(t *testing.T) {
	assert.True(t, ingest.BucketUnsafeForDeletion("aptrust.preservation"))
	assert.True(t, ingest.BucketUnsafeForDeletion("aptrust.test.preservation"))
	assert.True(t, ingest.BucketUnsafeForDeletion("aptrust.preservation-or"))
	assert.True(t, ingest.BucketUnsafeForDeletion("preservation.demo"))

	assert.False(t, ingest.BucketUnsafeForDeletion("staging"))
	assert.False(t, ingest.BucketUnsafeForDeletion("staging.test"))
}

func TestCleanAll(t *testing.T) {
	context := common.NewContext()
	bagPath := getBagPath("original", "test.edu.apt-002.tar")
	cleanup := prepareForCleanup(t, bagPath, cleanupItemID, context)
	require.NotNil(t, cleanup)

	files, _, _ := context.RedisClient.GetBatchOfFileKeys(cleanup.WorkItemID, 0, 100)
	fileCount := len(files)
	assert.True(t, fileCount > 0)

	assert.Equal(t, fileCount, stagingBucketFileCount(cleanup))

	filesDeleted, errors := cleanup.CleanAll()
	require.Empty(t, errors)
	assert.Equal(t, fileCount, filesDeleted)

	// Make sure no files remain in staging
	assert.Equal(t, 0, stagingBucketFileCount(cleanup))

	// Make sure no records remain in Redis
	obj, _ := context.RedisClient.IngestObjectGet(cleanup.WorkItemID, cleanup.IngestObject.Identifier())
	assert.Nil(t, obj)

	items, _, _ := context.RedisClient.GetBatchOfFileKeys(cleanup.WorkItemID, 0, 100)
	assert.Empty(t, items)
}

func stagingBucketFileCount(cleanup *ingest.Cleanup) int {
	fileCount := 0
	stagingBucket := cleanup.Context.Config.StagingBucket
	prefix := fmt.Sprintf("%d/", cleanup.WorkItemID)
	doneCh := make(chan struct{})
	defer close(doneCh)
	s3Client := cleanup.Context.S3Clients[constants.StorageProviderAWS]
	for _ = range s3Client.ListObjects(stagingBucket, prefix, true, doneCh) {
		fileCount++
	}
	return fileCount
}
