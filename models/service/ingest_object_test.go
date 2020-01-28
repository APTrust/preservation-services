package service_test

import (
	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func getObjectWithTags() *service.IngestObject {
	tags := make([]*bagit.Tag, 3)
	tags[0] = bagit.NewTag("bag-info.txt", "label1", "value1")
	tags[1] = bagit.NewTag("bag-info.txt", "label1", "value2")
	tags[2] = bagit.NewTag("aptrust-info.txt", "Access", "Institution")

	obj := testutil.GetIngestObject()
	obj.Tags = append(obj.Tags, tags...)
	return obj
}

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

func TestBaseNameOfS3Key(t *testing.T) {
	obj := service.NewIngestObject("bucket", "test-bag.b001.of200.tar", "\"123456\"", "test.edu", int64(500))
	assert.Equal(t, "test-bag.b001.of200", obj.BaseNameOfS3Key())

	obj.S3Key = "photos.tar"
	assert.Equal(t, "photos", obj.BaseNameOfS3Key())
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
	obj, err := service.IngestObjectFromJson(IngestObjectJson)
	assert.Nil(t, err)
	assert.Equal(t, expectedObj.Identifier(), obj.Identifier())
	assert.Equal(t, expectedObj.ParsableTagFiles, obj.ParsableTagFiles)
}

func TestObjToJson(t *testing.T) {
	obj := testutil.GetIngestObject()
	data, err := obj.ToJson()
	assert.Nil(t, err)
	assert.Equal(t, IngestObjectJson, data)
}

func TestGetTags(t *testing.T) {
	obj := getObjectWithTags()
	label1Tags := obj.GetTags("bag-info.txt", "label1")
	require.Equal(t, 2, len(label1Tags))
	for _, tag := range label1Tags {
		assert.Equal(t, "bag-info.txt", tag.TagFile)
		assert.Equal(t, "label1", tag.TagName)
	}
	assert.Equal(t, "value1", label1Tags[0].Value)
	assert.Equal(t, "value2", label1Tags[1].Value)

	assert.Equal(t, 1, len(obj.GetTags("aptrust-info.txt", "Access")))
	assert.Equal(t, 0, len(obj.GetTags("bag-info.txt", "Does-Not-Exist")))
}

func TestGetTag(t *testing.T) {
	obj := getObjectWithTags()
	tag := obj.GetTag("bag-info.txt", "label1")
	assert.Equal(t, "bag-info.txt", tag.TagFile)
	assert.Equal(t, "label1", tag.TagName)
	assert.Equal(t, "value1", tag.Value)

	tag = obj.GetTag("aptrust-info.txt", "Access")
	assert.Equal(t, "aptrust-info.txt", tag.TagFile)
	assert.Equal(t, "Access", tag.TagName)
	assert.Equal(t, "Institution", tag.Value)

	assert.Nil(t, obj.GetTag("bag-info.txt", "Does-Not-Exist"))
}

func TestBagItProfileFormat(t *testing.T) {
	obj := testutil.GetIngestObject()

	// If no BagIt-Profile-Identifier tag, should return default
	assert.Equal(t, constants.BagItProfileDefault, obj.BagItProfileFormat())

	// Set explicitly to APTrust profile
	tag := bagit.NewTag(
		"bag-info.txt",
		"BagIt-Profile-Identifier",
		"https://wiki.aptrust.org/APTrust_BagIt_Profile-2.2")
	obj.Tags = append(obj.Tags, tag)
	assert.Equal(t, constants.BagItProfileDefault, obj.BagItProfileFormat())

	// Set explicitly to BTR profile
	tag.Value = "https://raw.githubusercontent.com/dpscollaborative/btr_bagit_profile/master/btr-bagit-profile.json"
	assert.Equal(t, constants.BagItProfileBTR, obj.BagItProfileFormat())
}

const IngestObjectJson = `{"deleted_from_receiving_at":"1904-06-16T15:04:05Z","etag":"12345678","error_message":"No error","file_count":0,"has_fetch_txt":false,"id":555,"institution":"test.edu","manifests":["manifest-md5.txt","manifest-sha256.txt"],"parsable_tag_files":["bag-info.txt","aptrust-info.txt"],"s3_bucket":"aptrust.receiving.test.edu","s3_key":"some-bag.tar","serialization":"application/tar","size":99999,"storage_option":"Standard","tag_files":["bag-info.txt","aptrust-info.txt","misc/custom-tag-file.txt"],"tag_manifests":["tagmanifest-md5.txt","tagmanifest-sha256.txt"],"tags":[]}`
