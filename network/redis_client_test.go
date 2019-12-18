package network_test

import (
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewRedisClient(t *testing.T) {
	client := network.NewRedisClient("localhost:6379", "", 0)
	assert.NotNil(t, client)
}

func TestRedisPing(t *testing.T) {
	client := network.NewRedisClient("localhost:6379", "", 0)
	response, err := client.Ping()
	assert.Nil(t, err)
	assert.Equal(t, "PONG", response)
}

func TestIngestObjectSave(t *testing.T) {
	client := network.NewRedisClient("localhost:6379", "", 0)
	require.NotNil(t, client)
	obj := service.NewIngestObject("bucket", "key", "etag", "test.edu", int64(555))
	err := client.IngestObjectSave(9999, obj)
	assert.Nil(t, err)
}

func TestIngestObjectGet(t *testing.T) {
	client := network.NewRedisClient("localhost:6379", "", 0)
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
	client := network.NewRedisClient("localhost:6379", "", 0)
	require.NotNil(t, client)
	obj := service.NewIngestObject("bucket", "key", "etag", "test.edu", int64(555))
	err := client.IngestObjectSave(9999, obj)
	assert.Nil(t, err)

	err = client.IngestObjectDelete(9999, obj.Identifier())
	assert.Nil(t, err)
}
