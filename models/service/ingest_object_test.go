package service_test

import (
	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	assert.NotNil(t, obj.Tags)
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

func TestGetTags(t *testing.T) {
	tags := make([]*bagit.Tag, 3)
	tags[0] = bagit.NewTag("bag-info.txt", "label1", "value1")
	tags[1] = bagit.NewTag("bag-info.txt", "label1", "value2")
	tags[2] = bagit.NewTag("aptrust-info.txt", "Access", "Institution")

	obj := testutil.GetIngestObject()
	obj.Tags = append(obj.Tags, tags...)

	label1Tags := obj.GetTags("bag-info.txt", "label1")
	require.Equal(t, 2, len(label1Tags))
	for _, tag := range label1Tags {
		assert.Equal(t, "bag-info.txt", tag.SourceFile)
		assert.Equal(t, "label1", tag.Label)
	}
	assert.Equal(t, "value1", label1Tags[0].Value)
	assert.Equal(t, "value2", label1Tags[1].Value)

	assert.Equal(t, 1, len(obj.GetTags("aptrust-info.txt", "Access")))
	assert.Equal(t, 0, len(obj.GetTags("bag-info.txt", "Does-Not-Exist")))
}
