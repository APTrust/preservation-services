package network_test

import (
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
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
