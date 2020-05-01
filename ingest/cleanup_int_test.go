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
	assert.False(t, ingest.BucketUnsafeForDeletion("aptrust.receiving.test.edu"))
}

func TestCleanupRun(t *testing.T) {
	context := common.NewContext()
	bagPath := getBagPath("original", "test.edu.apt-002.tar")
	cleanup := prepareForCleanup(t, bagPath, cleanupItemID, context)
	require.NotNil(t, cleanup)

	// Make sure the object is in the receiving bucket before we
	// call cleanup.
	objInfo, err := context.S3StatObject(constants.StorageProviderAWS,
		cleanup.IngestObject.S3Bucket, cleanup.IngestObject.S3Key)
	assert.Nil(t, err)
	assert.NotNil(t, objInfo)
	assert.True(t, objInfo.Size > int64(0))

	// Note that the S3 staging bucket will have 5 files more
	// than Redis. This is because the staging bucket keeps an
	// extra copy of manifests and tag files. The extra copy is
	// used during bag validation, and can be used for forensics
	// if an ingest stalls or fails. The 5 files with duplicate
	// copies are:
	//
	// aptrust-info.txt
	// bag-info.txt
	// bagit.txt
	// manifest-md5.txt
	// tagmanifest-md5.txt
	files, _, _ := context.RedisClient.GetBatchOfFileKeys(cleanup.WorkItemID, 0, 100)
	fileCount := len(files) + 5
	assert.True(t, fileCount > 0)

	assert.Equal(t, fileCount, stagingBucketFileCount(cleanup))

	filesDeleted, errors := cleanup.Run()
	require.Empty(t, errors)
	assert.Equal(t, fileCount, filesDeleted)

	// Make sure no files remain in staging
	assert.Equal(t, 0, stagingBucketFileCount(cleanup))

	// Make sure no records remain in Redis
	obj, _ := context.RedisClient.IngestObjectGet(cleanup.WorkItemID, cleanup.IngestObject.Identifier())
	assert.Nil(t, obj)

	items, _, _ := context.RedisClient.GetBatchOfFileKeys(cleanup.WorkItemID, 0, 100)
	assert.Empty(t, items)

	// Make sure the object is NO LONGER in the receiving bucket
	// after we call cleanup.
	objInfo, err = context.S3StatObject(constants.StorageProviderAWS,
		cleanup.IngestObject.S3Bucket, cleanup.IngestObject.S3Key)
	require.NotNil(t, err)
	assert.Equal(t, "The specified key does not exist.", err.Error())
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
