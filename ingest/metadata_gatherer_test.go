package ingest_test

import (
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The setup/teardown functions for these tests, along with definitions
// for all the "goodbag" vars are in ingest_common_test.go.

func TestNewMetadataGatherer(t *testing.T) {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	g := ingest.NewMetadataGatherer(context, 9999, obj)
	require.NotNil(t, g)
	assert.Equal(t, context, g.Context)
	assert.Equal(t, 9999, g.WorkItemID)
	assert.Equal(t, obj, g.IngestObject)
}

func TestMetadataGathererRun(t *testing.T) {
	context := common.NewContext()
	setupS3(t, context, keyToGoodBag, pathToGoodBag)
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	g := ingest.NewMetadataGatherer(context, 9999, obj)

	fileCount, errors := g.Run()
	require.Empty(t, errors)
	assert.True(t, fileCount > 0)

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
		testIngestFile_MetadataGatherer(t, ingestFile)
	}
	testIngestObject_MetadataGatherer(t, context, objIdentifier)
}

func testIngestObject_MetadataGatherer(t *testing.T, context *common.Context, objIdentifier string) {
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

func testIngestFile_MetadataGatherer(t *testing.T, ingestFile *service.IngestFile) {
	assert.NotEmpty(t, ingestFile.UUID)
	assert.Equal(t, 0, len(ingestFile.StorageRecords))
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
		assert.Equal(t, 4, len(ingestFile.Checksums), ingestFile.Identifier())
	} else if filetype != constants.FileTypeTagManifest {
		assert.Equal(t, 6, len(ingestFile.Checksums), ingestFile.Identifier())
	} else {
		// Manifest files don't include manifest checksums
		assert.Equal(t, 4, len(ingestFile.Checksums), ingestFile.Identifier())
	}
	algs := []string{
		"md5",    // from ingest scan
		"sha1",   // from ingest scan
		"sha256", // from ingest scan
		"sha512", // from ingest scan
		"md5",    // from manifest
		"sha256", // from ingest scan
	}
	for i, checksum := range ingestFile.Checksums {
		testChecksum(t, checksum, algs[i], i)
	}
}

func testChecksum(t *testing.T, checksum *service.IngestChecksum, alg string, index int) {
	assert.Equal(t, alg, checksum.Algorithm)
	assert.NotEmpty(t, checksum.DateTime)
	assert.NotEqual(t, emptyTimeValue, checksum.DateTime)
	assert.NotEmpty(t, checksum.Digest)
	if index < 4 {
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

// Specifically test bug https://trello.com/c/9E356dMX,
// which showed all items ingested into staging were being marked
// with StorageOption Standard, even when the Storage-Option
// tag in aptrust-info.txt specified a glacier option.
func TestStorageOptionIsSetCorrectly(t *testing.T) {
	context := common.NewContext()

	bags := []struct {
		Name          string
		StorageOption string
		Md5Sum        string
	}{
		{
			Name:          "example.edu.sample_glacier_oh.tar",
			StorageOption: constants.StorageGlacierOH,
			Md5Sum:        "de11380fd69842f3a81cf803c6198d11",
		},
		{
			Name:          "example.edu.sample_glacier_or.tar",
			StorageOption: constants.StorageGlacierOR,
			Md5Sum:        "398fb79ec31b4f12d6e4a12dd9570d69",
		},
		{
			Name:          "example.edu.sample_glacier_va.tar",
			StorageOption: constants.StorageGlacierVA,
			Md5Sum:        "73232aac4ebcb95cb71f8094094596d4",
		},
		{
			Name:          "test.edu.btr-glacier-deep-oh.tar",
			StorageOption: constants.StorageGlacierDeepOH,
			Md5Sum:        "4d176a64a5395d71683c678f43c7b423",
		},
		{
			Name:          "test.edu.btr-wasabi-or.tar",
			StorageOption: constants.StorageWasabiOR,
			Md5Sum:        "599ec9589e7a17bddb51506911b64762",
		},
	}

	for i, bag := range bags {
		pathToBag := testutil.PathToUnitTestBag(bag.Name)
		setupS3(t, context, bag.Name, pathToBag)
		obj := getIngestObject(pathToBag, bag.Md5Sum)
		g := ingest.NewMetadataGatherer(context, int64(700+i), obj)

		fileCount, errors := g.Run()
		require.Empty(t, errors)
		assert.True(t, fileCount > 0)

		assert.Equal(t, bag.StorageOption, obj.StorageOption)
	}
}
