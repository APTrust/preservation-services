package service_test

import (
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewIngestObject(t *testing.T) {
	obj := service.NewIngestObject("bucket", "test-bag.b001.of200.tar", "\"123456\"", "test.edu", int64(500))
	assert.Equal(t, "123456", obj.ETag)
	assert.Equal(t, "test.edu/test-bag", obj.Identifier())
	assert.Equal(t, "test.edu", obj.Institution)
	assert.NotNil(t, obj.Manifests)
	assert.NotNil(t, obj.ParsableTagFiles)
	assert.Equal(t, "bucket", obj.S3Bucket)
	assert.Equal(t, "test-bag.b001.of200.tar", obj.S3Key)
	assert.EqualValues(t, 500, obj.Size)
	assert.NotNil(t, obj.TagManifests)
	assert.NotNil(t, obj.TopLevelDirs)
}

func TestIngestObjectBagName(t *testing.T) {
	obj := service.NewIngestObject("bucket", "test-bag.b001.of200.tar", "\"123456\"", "test.edu", int64(500))
	assert.Equal(t, "test-bag", obj.BagName())

	obj.S3Key = "photos.tar"
	assert.Equal(t, "photos", obj.BagName())
}

func TestIngestObjectIdentifier(t *testing.T) {
	obj := service.NewIngestObject("bucket", "test-bag.b001.of200.tar", "\"123456\"", "test.edu", int64(500))
	assert.Equal(t, "test.edu/test-bag", obj.Identifier())

	obj.Institution = "example.edu"
	obj.S3Key = "photos.tar"
	assert.Equal(t, "example.edu/photos", obj.Identifier())
}

func TestObjFromJson(t *testing.T) {
	expectedObj := testutil.GetIngestObject()
	obj, err := service.IngestObjectFromJson(testutil.IngestObjectJson)
	assert.Nil(t, err)
	assert.Equal(t, expectedObj.Identifier(), obj.Identifier())
	assert.Equal(t, expectedObj.ParsableTagFiles, obj.ParsableTagFiles)
}

func TestObjToJson(t *testing.T) {
	obj := testutil.GetIngestObject()
	data, err := obj.ToJson()
	assert.Nil(t, err)
	assert.Equal(t, testutil.IngestObjectJson, data)
}
