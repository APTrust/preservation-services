package ingest_test

import (
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"strings"
	"testing"
)

// The setup/teardown functions for these tests, along with definitions
// for all the "goodbag" vars are in ingest_common_test.go.

func TestNewMetadataGatherer(t *testing.T) {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	g := ingest.NewMetadataGatherer(context, 9999, obj)
	require.NotNil(t, g)
	assert.Equal(t, context, g.Context)
	assert.Equal(t, 9999, g.WorkItemId)
	assert.Equal(t, obj, g.IngestObject)
}

func TestGetS3Object(t *testing.T) {
	context := common.NewContext()
	setupS3(t, context, keyToGoodBag, pathToGoodBag)
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	g := ingest.NewMetadataGatherer(context, 9999, obj)

	minioObj, err := g.GetS3Object()
	require.NotNil(t, minioObj)
	defer minioObj.Close()
	require.Nil(t, err)
	assert.NotNil(t, minioObj)

	stats, err := minioObj.Stat()
	require.Nil(t, err)
	assert.Equal(t, stats.Size, goodbagSize)
}

func TestScanBag(t *testing.T) {
	context := common.NewContext()
	setupS3(t, context, keyToGoodBag, pathToGoodBag)
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	g := ingest.NewMetadataGatherer(context, 9999, obj)

	err := g.ScanBag()
	require.Nil(t, err)

	testS3Files(t, context)
	testRedisRecords(t, context, obj.Identifier())

	// TODO: Clean up files and Redis records here?
}

func testRedisRecords(t *testing.T, context *common.Context, objIdentifier string) {
	// Make sure all expected records are in local redis server.
	allFilesInBag := append(goodbagS3Files, goodbagOtherFiles...)
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
	assert.Equal(t, goodbagMd5, obj.ETag)
	assert.Equal(t, "example.edu", obj.Institution)
	assert.Equal(t, "receiving", obj.S3Bucket)
	assert.Equal(t, keyToGoodBag, obj.S3Key)
	assert.Equal(t, goodbagSize, obj.Size)
	assert.Equal(t, "receiving", obj.S3Bucket)
	assert.Equal(t, 16, obj.FileCount)

	// Note that our test bag, created with older bagging software,
	// is missing the Storage-Option tag. This is common among our
	// depositors. The MetadataGatherer adds the tag if it's missing,
	// with the documented default value of "Standard".
	require.Equal(t, 11, len(obj.Tags))
	for _, tag := range obj.Tags {
		assert.NotEmpty(t, tag.TagName)
		assert.NotEmpty(t, tag.TagName)
		assert.NotEmpty(t, tag.Value)
	}
	// Spot check one tag
	tag := obj.Tags[4]
	assert.Equal(t, "bag-info.txt", tag.TagFile)
	assert.Equal(t, "Bag-Count", tag.TagName)
	assert.Equal(t, "1 of 1", tag.Value)

	// Check the Storage-Option tag
	storageOptionTag := obj.Tags[10]
	assert.Equal(t, "aptrust-info.txt", storageOptionTag.TagFile)
	assert.Equal(t, "Storage-Option", storageOptionTag.TagName)
	assert.Equal(t, "Standard", storageOptionTag.Value)

	// Confirm metafile paths
	require.Equal(t, 2, len(obj.Manifests))
	assert.Equal(t, "md5", obj.Manifests[0])
	assert.Equal(t, "sha256", obj.Manifests[1])

	require.Equal(t, 2, len(obj.TagManifests))
	assert.Equal(t, "md5", obj.TagManifests[0])
	assert.Equal(t, "sha256", obj.TagManifests[1])

	require.Equal(t, 3, len(obj.ParsableTagFiles))
	assert.Equal(t, "aptrust-info.txt", obj.ParsableTagFiles[0])
	assert.Equal(t, "bag-info.txt", obj.ParsableTagFiles[1])
	assert.Equal(t, "bagit.txt", obj.ParsableTagFiles[2])

	require.Equal(t, len(goodbagTagFiles), len(obj.TagFiles))
	for i, filename := range obj.TagFiles {
		assert.Equal(t, goodbagTagFiles[i], filename)
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
		assert.True(t, (checksum.Source == constants.SourceManifest ||
			checksum.Source == constants.SourceTagManifest))
	}
}

func testS3Files(t *testing.T, context *common.Context) {
	// Make sure all expected files were copied to local S3 server.
	for i, file := range goodbagS3Files {
		fullpath := path.Join(context.Config.BaseWorkingDir,
			"minio", "staging", "9999", file)
		require.True(t, util.FileExists(fullpath))
		stats, err := os.Stat(fullpath)
		require.Nil(t, err)
		assert.Equal(t, goodbags3FileSizes[i], stats.Size())
	}
}
