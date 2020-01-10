package ingest_test

import (
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
)

var key = "example.edu.tagsample_good.tar"
var testbag = testutil.PathToUnitTestBag(key)
var testbagMd5 = "f4323e5e631834c50d077fc3e03c2fed"
var testbagSize = int64(40960)
var s3files = []string{
	"aptrust-info.txt",
	"bag-info.txt",
	"bagit.txt",
	"manifest-md5.txt",
	"manifest-sha256.txt",
	"tagmanifest-md5.txt",
	"tagmanifest-sha256.txt",
}
var s3FileSizes = []int64{
	int64(67),
	int64(297),
	int64(55),
	int64(230),
	int64(358),
	int64(438),
	int64(694),
}
var otherFilesInBag = []string{
	"data/datastream-DC",
	"data/datastream-descMetadata",
	"data/datastream-MARC",
	"data/datastream-RELS-EXT",
	"custom_tags/tracked_file_custom.xml",
	"custom_tags/tracked_tag_file.txt",
	"custom_tags/untracked_tag_file.txt",
}

var allTagFiles = []string{
	"aptrust-info.txt",
	"bag-info.txt",
	"bagit.txt",
	"custom_tag_file.txt",
	"junk_file.txt",
	"custom_tags/tracked_file_custom.xml",
	"custom_tags/tracked_tag_file.txt",
	"custom_tags/untracked_tag_file.txt",
}

const emptyTimeValue = "0001-01-01 00:00:00 +0000 UTC"

// Make sure the bag we want to work on is in S3 before we
// start our tests.
func setupS3(t *testing.T, context *common.Context) {
	clearS3Files(t, context)
	putBagInS3(t, context)
}

// Get rid of text files that may be lingering in our local
// in-memory S3 server from the previous test.
func clearS3Files(t *testing.T, context *common.Context) {
	for _, filename := range s3files {
		key := fmt.Sprintf("9999/%s", filename)
		_ = context.S3Clients[constants.S3ClientAWS].RemoveObject(
			constants.TestBucketReceiving,
			key)
		//require.Nil(t, err)
	}
}

// Copy testbag to local in-memory S3 service.
func putBagInS3(t *testing.T, context *common.Context) {
	// Uncomment the following to get a full printout
	// of the client's HTTP exchanges on Stderr.
	//context.S3Clients[constants.S3ClientAWS].TraceOn(os.Stderr)

	bytesWritten, err := context.S3Clients[constants.S3ClientAWS].FPutObject(
		constants.TestBucketReceiving,
		key,
		testbag,
		minio.PutObjectOptions{})
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	require.Nil(t, err, msg)
	assert.True(t, (bytesWritten >= testbagSize))
}

// Returns an IngestObject that describes the tarred bag waiting
// in our receiving bucket.
func getIngestObject() *service.IngestObject {
	return service.NewIngestObject(
		constants.TestBucketReceiving, // bucket
		filepath.Base(testbag),        // key
		testbagMd5,                    // eTag
		"example.edu",                 // institution
		testbagSize,                   // size
	)
}

func TestNewMetadataGatherer(t *testing.T) {
	context := common.NewContext()
	obj := getIngestObject()
	g := ingest.NewMetadataGatherer(context, 9999, obj)
	require.NotNil(t, g)
	assert.Equal(t, context, g.Context)
	assert.Equal(t, 9999, g.WorkItemId)
	assert.Equal(t, obj, g.IngestObject)
}

func TestGetS3Object(t *testing.T) {
	context := common.NewContext()
	setupS3(t, context)
	obj := getIngestObject()
	g := ingest.NewMetadataGatherer(context, 9999, obj)

	minioObj, err := g.GetS3Object()
	require.NotNil(t, minioObj)
	defer minioObj.Close()
	require.Nil(t, err)
	assert.NotNil(t, minioObj)

	stats, err := minioObj.Stat()
	require.Nil(t, err)
	assert.Equal(t, stats.Size, testbagSize)
}

func TestScanBag(t *testing.T) {
	context := common.NewContext()
	setupS3(t, context)
	obj := getIngestObject()
	g := ingest.NewMetadataGatherer(context, 9999, obj)

	err := g.ScanBag()
	require.Nil(t, err)

	testS3Files(t, context)
	testRedisRecords(t, context, obj.Identifier())

	// TODO: Clean up files and Redis records here?
}

func testRedisRecords(t *testing.T, context *common.Context, objIdentifier string) {
	// Make sure all expected records are in local redis server.
	allFilesInBag := append(s3files, otherFilesInBag...)
	for _, f := range allFilesInBag {
		// force forward slashes
		fullpath := fmt.Sprintf("example.edu/example.edu.tagsample_good/%s", f)
		ingestFile, err := context.RedisClient.IngestFileGet(9999, fullpath)
		require.Nil(t, err)
		require.NotNil(t, ingestFile)
		testIngestFile(t, ingestFile)
	}
	testIngestObject(t, context, objIdentifier)
}

func testIngestObject(t *testing.T, context *common.Context, objIdentifier string) {
	obj, err := context.RedisClient.IngestObjectGet(9999, objIdentifier)
	require.Nil(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, testbagMd5, obj.ETag)
	assert.Equal(t, "example.edu", obj.Institution)
	assert.Equal(t, "receiving", obj.S3Bucket)
	assert.Equal(t, key, obj.S3Key)
	assert.Equal(t, testbagSize, obj.Size)
	assert.Equal(t, "receiving", obj.S3Bucket)
	require.Equal(t, 10, len(obj.Tags))
	for _, tag := range obj.Tags {
		assert.NotEmpty(t, tag.SourceFile)
		assert.NotEmpty(t, tag.Label)
		assert.NotEmpty(t, tag.Value)
	}
	// Spot check one tag
	tag := obj.Tags[4]
	assert.Equal(t, "bag-info.txt", tag.SourceFile)
	assert.Equal(t, "Bag-Count", tag.Label)
	assert.Equal(t, "1 of 1", tag.Value)

	// Confirm metafile paths
	require.Equal(t, 2, len(obj.Manifests))
	assert.Equal(t, "manifest-md5.txt", obj.Manifests[0])
	assert.Equal(t, "manifest-sha256.txt", obj.Manifests[1])

	require.Equal(t, 2, len(obj.TagManifests))
	assert.Equal(t, "tagmanifest-md5.txt", obj.TagManifests[0])
	assert.Equal(t, "tagmanifest-sha256.txt", obj.TagManifests[1])

	require.Equal(t, 3, len(obj.ParsableTagFiles))
	assert.Equal(t, "aptrust-info.txt", obj.ParsableTagFiles[0])
	assert.Equal(t, "bag-info.txt", obj.ParsableTagFiles[1])
	assert.Equal(t, "bagit.txt", obj.ParsableTagFiles[2])

	require.Equal(t, len(allTagFiles), len(obj.TagFiles))
	for i, filename := range obj.TagFiles {
		assert.Equal(t, allTagFiles[i], filename)
	}
}

func testIngestFile(t *testing.T, ingestFile *service.IngestFile) {
	assert.NotEmpty(t, ingestFile.UUID)
	assert.Empty(t, ingestFile.StorageRecords)
	assert.Equal(t, "Standard", ingestFile.StorageOption)
	assert.True(t, ingestFile.Size > 0)
	assert.NotEmpty(t, ingestFile.PathInBag)
	assert.Equal(t, "example.edu/example.edu.tagsample_good", ingestFile.ObjectIdentifier)
	assert.True(t, ingestFile.NeedsSave)
	filetype := ingestFile.FileType()
	if strings.HasSuffix(ingestFile.Identifier(), "untracked_tag_file.txt") {
		// Untracked tag file does not appear in manifests.
		// This is a legal case per the BagIt spec.
		// TODO: Is there a reliable way to identify untracked tag files?
		assert.Equal(t, 2, len(ingestFile.Checksums), ingestFile.Identifier())
	} else if filetype != constants.FileTypeTagManifest {
		assert.Equal(t, 4, len(ingestFile.Checksums), ingestFile.Identifier())
	} else {
		// Manifest files don't include manifest checksums
		assert.Equal(t, 2, len(ingestFile.Checksums), ingestFile.Identifier())
	}
	for i, checksum := range ingestFile.Checksums {
		alg := "md5"
		if i%2 == 1 {
			alg = "sha256"
		}
		testChecksum(t, checksum, alg, i)
	}
}

func testChecksum(t *testing.T, checksum *service.IngestChecksum, alg string, index int) {
	assert.Equal(t, alg, checksum.Algorithm)
	assert.NotEmpty(t, checksum.DateTime)
	assert.NotEqual(t, emptyTimeValue, checksum.DateTime)
	assert.NotEmpty(t, checksum.Digest)
	if index < 2 {
		assert.Equal(t, "ingest", checksum.Source)
	} else {
		assert.Equal(t, "manifest", checksum.Source)
	}
}

func testS3Files(t *testing.T, context *common.Context) {
	// Make sure all expected files were copied to local S3 server.
	for i, file := range s3files {
		fullpath := path.Join(context.Config.BaseWorkingDir,
			"minio", "staging", "9999", file)
		require.True(t, util.FileExists(fullpath))
		stats, err := os.Stat(fullpath)
		require.Nil(t, err)
		assert.Equal(t, s3FileSizes[i], stats.Size())
	}
}
