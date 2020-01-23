package network_test

import (
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	//"time"
)

func getRedisClient() *network.RedisClient {
	return network.NewRedisClient(
		constants.TestRedisServerURL,
		constants.TestRedisPwd,
		constants.TestRedisDB,
	)
}

func TestNewRedisClient(t *testing.T) {
	client := getRedisClient()
	assert.NotNil(t, client)
}

func TestRedisPing(t *testing.T) {
	client := getRedisClient()
	response, err := client.Ping()
	assert.Nil(t, err)
	assert.Equal(t, "PONG", response)
}

func TestIngestObjectSaveAndGet(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)
	obj := service.NewIngestObject("bucket", "key", "etag", "test.edu", int64(555))
	err := client.IngestObjectSave(9999, obj)
	assert.Nil(t, err)

	retrievedObj, err := client.IngestObjectGet(9999, obj.Identifier())
	assert.Nil(t, err)
	assert.NotNil(t, retrievedObj)
	assert.Equal(t, obj.ETag, retrievedObj.ETag)
	assert.Equal(t, obj.S3Bucket, retrievedObj.S3Bucket)
	assert.Equal(t, obj.S3Key, retrievedObj.S3Key)
}

func TestIngestObjectDelete(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)
	obj := service.NewIngestObject("bucket", "key", "etag", "test.edu", int64(555))
	err := client.IngestObjectSave(9999, obj)
	assert.Nil(t, err)

	err = client.IngestObjectDelete(9999, obj.Identifier())
	assert.Nil(t, err)
}

func TestIngestFileSaveAndGet(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)
	f := service.NewIngestFile("test.edu/bag1", "data/images.photo.jpg")
	err := client.IngestFileSave(9999, f)
	assert.Nil(t, err)

	retrievedFile, err := client.IngestFileGet(9999, f.Identifier())
	assert.Nil(t, err)
	assert.NotNil(t, retrievedFile)
	assert.Equal(t, f.ObjectIdentifier, retrievedFile.ObjectIdentifier)
	assert.Equal(t, f.PathInBag, retrievedFile.PathInBag)
}

func TestIngestFileDelete(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)
	f := service.NewIngestFile("test.edu/bag1", "data/images.photo.jpg")
	err := client.IngestFileSave(9999, f)
	assert.Nil(t, err)

	err = client.IngestFileDelete(9999, f.Identifier())
	assert.Nil(t, err)
}

func TestWorkItemDelete(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)

	// Save an object...
	obj := service.NewIngestObject("bucket", "bag1.tar",
		"etag", "test.edu", int64(555))
	err := client.IngestObjectSave(9999, obj)
	assert.Nil(t, err)

	// Make sure it's there.
	objFromRedis, err := client.IngestObjectGet(9999, "test.edu/bag1")
	assert.NotNil(t, objFromRedis)
	assert.Nil(t, err)

	// and some files...
	files := []string{
		"data/images.photo01.jpg",
		"data/images.photo02.jpg",
		"data/images.photo03.jpg",
		"data/images.photo04.jpg",
	}
	for _, filename := range files {
		f := service.NewIngestFile("test.edu/bag1", filename)
		err = client.IngestFileSave(9999, f)
		assert.Nil(t, err)
	}

	// Make sure files are there.
	for _, filename := range files {
		fileIdentifier := fmt.Sprintf("test.edu/bag1/%s", filename)
		f, err := client.IngestFileGet(9999, fileIdentifier)
		assert.NotNil(t, f)
		assert.Nil(t, err)
	}

	// Call delete to get rid of all records associated with this
	// WorkItem.
	itemsDeleted, err := client.WorkItemDelete(9999)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, itemsDeleted)

	// Make sure the IngestObject record was actually deleted.
	objFromRedis, err = client.IngestObjectGet(9999, "test.edu/bag1")
	assert.Nil(t, objFromRedis)
	assert.Equal(t, "IngestObjectGet (9999, test.edu/bag1): redis: nil", err.Error())

	// Make sure all of the IngestFile objects were deleted.
	for _, filename := range files {
		fileIdentifier := fmt.Sprintf("test.edu/bag1/%s", filename)
		f, err := client.IngestFileGet(9999, fileIdentifier)
		assert.Nil(t, f)
		assert.NotNil(t, err)
	}
}

func TestGetBatchOfFileKeys(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)

	testGetBatch(t, client, 10, 3)
	testGetBatch(t, client, 100, 12)
}

func testGetBatch(t *testing.T, client *network.RedisClient, totalItems, batchSize int) {
	for i := 0; i < totalItems; i++ {
		f := service.NewIngestFile("test.edu/bag1",
			fmt.Sprintf("file_%d.jpg", i))
		err := client.IngestFileSave(5555, f)
		assert.Nil(t, err)
	}

	defer client.WorkItemDelete(5555)

	//time.Sleep(250 * time.Millisecond)

	nextOffset := uint64(0)
	count := 0
	var fileMap map[string]*service.IngestFile
	var err error
	for {
		fileMap, nextOffset, err = client.GetBatchOfFileKeys(
			5555, nextOffset, int64(batchSize))
		require.Nil(t, err)
		count += len(fileMap)
		if nextOffset == 0 {
			break
		}
	}
	assert.Equal(t, totalItems, count)
}

func TestWorkResultSaveAndGet(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)
	result := service.NewWorkResult(constants.OpIngestGatherMeta)
	result.AddError("fatal error", true)
	err := client.WorkResultSave(9999, result)
	assert.Nil(t, err)

	retrievedResult, err := client.WorkResultGet(9999, result.Operation)
	assert.Nil(t, err)
	assert.NotNil(t, retrievedResult)
	assert.Equal(t, constants.OpIngestGatherMeta, retrievedResult.Operation)
	assert.Equal(t, 1, len(retrievedResult.Errors))
	assert.Equal(t, "fatal error", retrievedResult.Errors[0])
	assert.True(t, retrievedResult.ErrorIsFatal)
}

func TestWorkResultSaveAndDelete(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)
	result := service.NewWorkResult(constants.OpIngestGatherMeta)
	err := client.WorkResultSave(9999, result)
	assert.Nil(t, err)

	retrievedResult, err := client.WorkResultGet(9999, result.Operation)
	assert.Nil(t, err)
	assert.NotNil(t, retrievedResult)

	err = client.WorkResultDelete(9999, constants.OpIngestGatherMeta)
	require.Nil(t, err)

	deletedResult, _ := client.WorkResultGet(9999, result.Operation)
	assert.Nil(t, deletedResult)
}
