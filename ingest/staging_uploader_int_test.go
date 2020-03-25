// +build integration

package ingest_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

const tarHeaderName = "example.edu.tagsample_good/data/datastream-descMetadata"
const filePathInBag = "data/datastream-descMetadata"
const objectIdentifier = "example.edu/example.edu.tagsample_good"
const fileIdentifier = "example.edu/example.edu.tagsample_good/data/datastream-descMetadata"

var gfIdentifiers = []string{
	"example.edu/example.edu.tagsample_good/aptrust-info.txt",
	"example.edu/example.edu.tagsample_good/bag-info.txt",
	"example.edu/example.edu.tagsample_good/bagit.txt",
	"example.edu/example.edu.tagsample_good/custom_tag_file.txt",
	"example.edu/example.edu.tagsample_good/junk_file.txt",
	"example.edu/example.edu.tagsample_good/manifest-md5.txt",
	"example.edu/example.edu.tagsample_good/manifest-sha256.txt",
	"example.edu/example.edu.tagsample_good/tagmanifest-md5.txt",
	"example.edu/example.edu.tagsample_good/tagmanifest-sha256.txt",
	"example.edu/example.edu.tagsample_good/data/datastream-DC",
	"example.edu/example.edu.tagsample_good/data/datastream-descMetadata",
	"example.edu/example.edu.tagsample_good/data/datastream-MARC",
	"example.edu/example.edu.tagsample_good/data/datastream-RELS-EXT",
	"example.edu/example.edu.tagsample_good/custom_tags/tracked_file_custom.xml",
	"example.edu/example.edu.tagsample_good/custom_tags/tracked_tag_file.txt",
	"example.edu/example.edu.tagsample_good/custom_tags/untracked_tag_file.txt",
}

func TestNewStagingUploader(t *testing.T) {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	uploader := ingest.NewStagingUploader(context, testWorkItemId, obj)
	require.NotNil(t, uploader)
	assert.Equal(t, context, uploader.Context)
	assert.Equal(t, testWorkItemId, uploader.WorkItemID)
	assert.Equal(t, obj, uploader.IngestObject)
}

func TestStagingUploader_GetS3Object(t *testing.T) {
	context := common.NewContext()
	setupS3(t, context, keyToGoodBag, pathToGoodBag)
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	uploader := ingest.NewStagingUploader(context, testWorkItemId, obj)
	s3Obj, err := uploader.GetS3Object()
	require.Nil(t, err)
	require.NotNil(t, s3Obj)
}

func TestCopyFilesToStaging(t *testing.T) {
	context := common.NewContext()
	uploader := prepareForCopyToStaging(t, context)

	testIdentifierGetFileAndPutOptions(t, uploader)

	err := uploader.CopyFilesToStaging()
	require.Nil(t, err)
}

// There's a lot of setup required to get to these functions,
// so let's test them together. None affects the others, so
// the fact that the tests are grouped has no bearing on the
// outcome.
func testIdentifierGetFileAndPutOptions(t *testing.T, uploader *ingest.StagingUploader) {
	// Test GetGenericFileIdentifier
	identifier, err := uploader.GetGenericFileIdentifier(tarHeaderName)
	require.Nil(t, err)
	assert.Equal(t, fileIdentifier, identifier)

	// Test GetIngestFile
	ingestFile, err := uploader.GetIngestFile(tarHeaderName)
	require.Nil(t, err)
	require.NotNil(t, ingestFile)
	assert.Equal(t, fileIdentifier, ingestFile.Identifier())
	assert.Equal(t, objectIdentifier, ingestFile.ObjectIdentifier)
	assert.Equal(t, filePathInBag, ingestFile.PathInBag)
	assert.Equal(t, int64(6191), ingestFile.Size)
}

func stagingPostTestS3AndRedis(t *testing.T, context *common.Context) {
	for _, identifier := range gfIdentifiers {
		ingestFile, err := context.RedisClient.IngestFileGet(testWorkItemId, identifier)
		require.Nil(t, err)
		require.NotNil(t, ingestFile)

		// Make sure the Redis record has a valid timestamp
		// saying when this file was copied to staging.
		assert.False(t, ingestFile.CopiedToStagingAt.IsZero())

		// Make sure the object was copied to S3 staging.
		s3ObjInfo, err := context.S3Clients[constants.S3ClientAWS].StatObject(
			context.Config.StagingBucket,
			ingestFile.UUID,
			minio.StatObjectOptions{})
		require.Nil(t, err)
		require.NotNil(t, s3ObjInfo)

		// Now make sure the metadata was set correctly in S3.
		md5 := ingestFile.GetChecksum(constants.SourceIngest, constants.AlgMd5)
		sha256 := ingestFile.GetChecksum(constants.SourceIngest, constants.AlgSha256)
		assert.Equal(t, "example.edu", s3ObjInfo.Metadata["institution"])
		assert.Equal(t, objectIdentifier, s3ObjInfo.Metadata["bag"])
		assert.Equal(t, ingestFile.PathInBag, s3ObjInfo.Metadata["bagpath"])
		assert.Equal(t, md5.Digest, s3ObjInfo.Metadata["md5"])
		assert.Equal(t, sha256.Digest, s3ObjInfo.Metadata["sha256"])
		assert.Equal(t, ingestFile.FileFormat, s3ObjInfo.ContentType)
	}
}
