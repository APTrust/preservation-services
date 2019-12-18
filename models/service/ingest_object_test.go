package service_test

import (
	"github.com/APTrust/preservation-services/models/service"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var timestamp, _ = time.Parse(time.RFC3339, "2020-01-02T15:04:05Z")

const objJson = `{"deleted_from_receiving_at":"2020-01-02T15:04:05Z","etag":"12345678","error_message":"No error","id":555,"identifier":"test.edu/some-bag","institution":"test.edu","manifests":["manifest-md5.txt","manifest-sha256.txt"],"parsable_tag_files":["bag-info.txt","aptrust-info.txt"],"s3_bucket":"aptrust.receiving.test.edu","s3_key":"some-bag.tar","size":99999,"storage_option":"Standard","tag_manifests":["tagmanifest-md5.txt","tagmanifest-sha256.txt"],"top_level_dirs":["some-bag"]}`

func getObject() *service.IngestObject {
	return &service.IngestObject{
		DeletedFromReceivingAt: timestamp,
		ETag:                   "12345678",
		ErrorMessage:           "No error",
		Id:                     555,
		Identifier:             "test.edu/some-bag",
		Institution:            "test.edu",
		Manifests:              []string{"manifest-md5.txt", "manifest-sha256.txt"},
		ParsableTagFiles:       []string{"bag-info.txt", "aptrust-info.txt"},
		S3Bucket:               "aptrust.receiving.test.edu",
		S3Key:                  "some-bag.tar",
		Size:                   99999,
		StorageOption:          "Standard",
		TagManifests:           []string{"tagmanifest-md5.txt", "tagmanifest-sha256.txt"},
		TopLevelDirs:           []string{"some-bag"},
	}
}

func TestNewIngestObject(t *testing.T) {
	obj := service.NewIngestObject("bucket", "test-bag.b001.of200.tar", "\"123456\"", "test.edu", int64(500))
	assert.Equal(t, "123456", obj.ETag)
	assert.Equal(t, "test.edu/test-bag", obj.Identifier)
	assert.Equal(t, "test.edu", obj.Institution)
	assert.NotNil(t, obj.Manifests)
	assert.NotNil(t, obj.ParsableTagFiles)
	assert.Equal(t, "bucket", obj.S3Bucket)
	assert.Equal(t, "test-bag.b001.of200.tar", obj.S3Key)
	assert.EqualValues(t, 500, obj.Size)
	assert.NotNil(t, obj.TagManifests)
	assert.NotNil(t, obj.TopLevelDirs)
}

func TestObjFromJson(t *testing.T) {
	expectedObj := getObject()
	obj, err := service.IngestObjectFromJson(objJson)
	assert.Nil(t, err)
	assert.Equal(t, expectedObj.Identifier, obj.Identifier)
	assert.Equal(t, expectedObj.ParsableTagFiles, obj.ParsableTagFiles)
}

func TestObjToJson(t *testing.T) {
	obj := getObject()
	data, err := obj.ToJson()
	assert.Nil(t, err)
	assert.Equal(t, objJson, data)
}
