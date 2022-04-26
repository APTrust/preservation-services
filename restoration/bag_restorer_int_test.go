//go:build integration
// +build integration

package restoration_test

import (
	ctx "context"
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"strings"
	"testing"

	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/restoration"
	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var bagRestorerSetupCompleted = false

// These ids are in the test fixture data
const aptrustObject = "test.edu/apt-test-restore"
const btrObject = "test.edu/btr-512-test-restore"

type RestorationItem struct {
	WorkItemID    int64
	ObjIdentifier string
	BagItProfile  string
}

var itemsToRestore = []RestorationItem{
	RestorationItem{
		WorkItemID:    87777,
		ObjIdentifier: aptrustObject,
		BagItProfile:  constants.BagItProfileDefault,
	},
	RestorationItem{
		WorkItemID:    87999,
		ObjIdentifier: btrObject,
		BagItProfile:  constants.BagItProfileBTR,
	},
}

// These files should be in the restored APTrust bag.
var expectedAPTrustFiles = []string{
	"apt-test-restore/bagit.txt",
	"apt-test-restore/data/sample.xml",
	"apt-test-restore/data/sample.json",
	"apt-test-restore/bag-info.txt",
	"apt-test-restore/aptrust-info.txt",
	"apt-test-restore/manifest-md5.txt",
	"apt-test-restore/manifest-sha256.txt",
	"apt-test-restore/tagmanifest-md5.txt",
	"apt-test-restore/tagmanifest-sha256.txt",
}

// These files should be in the restored BTR bag.
var expectedBTRFiles = []string{
	"btr-512-test-restore/bagit.txt",
	"btr-512-test-restore/data/sample.xml",
	"btr-512-test-restore/data/sample.json",
	"btr-512-test-restore/bag-info.txt",
	"btr-512-test-restore/manifest-sha1.txt",
	"btr-512-test-restore/manifest-sha256.txt",
	"btr-512-test-restore/manifest-sha512.txt",
	"btr-512-test-restore/tagmanifest-sha1.txt",
	"btr-512-test-restore/tagmanifest-sha256.txt",
	"btr-512-test-restore/tagmanifest-sha512.txt",
}

// setup ensures the files we want to restore are in the local Minio
// preservation buckets. All other info pertaining to these files/bags
// is loaded from fixture data into Registry by the test script in
// scripts/test.rb
func setup(t *testing.T, context *common.Context) {
	if bagRestorerSetupCompleted {
		return
	}

	s3Client := context.S3Clients[constants.StorageProviderAWS]

	// Our test files should be in these two preservation buckets,
	// according to the Registry fixture data.
	preservationBuckets := []string{
		context.Config.BucketStandardVA,
		context.Config.BucketStandardOR,
	}

	// Copy the files from int_test_bags/restoration/files to the
	// local Minio preservation buckets.
	dir := path.Join(testutil.PathToTestData(), "int_test_bags", "restoration", "files")
	files, err := ioutil.ReadDir(dir)
	require.Nil(t, err)
	for _, file := range files {
		fullpath := path.Join(dir, file.Name())
		for _, bucket := range preservationBuckets {
			_, err := s3Client.FPutObject(
				ctx.Background(),
				bucket,
				file.Name(),
				fullpath,
				minio.PutObjectOptions{})
			require.Nil(t, err)
		}
	}
	bagRestorerSetupCompleted = true
}

func getRestorationObject(t *testing.T, itemIdentifier, itemType string) *service.RestorationObject {
	profile := constants.DefaultProfileIdentifier
	if itemIdentifier == btrObject {
		profile = constants.BTRProfileIdentifier
	}

	var itemID int64
	var itemSize int64
	if itemType == constants.RestorationTypeObject {
		resp := common.NewContext().RegistryClient.IntellectualObjectByIdentifier(itemIdentifier)
		obj := resp.IntellectualObject()
		assert.Nil(t, resp.Error)
		assert.NotNil(t, obj)
		itemID = obj.ID
		itemSize = obj.Size
	} else {
		resp := common.NewContext().RegistryClient.GenericFileByIdentifier(itemIdentifier)
		gf := resp.GenericFile()
		assert.Nil(t, resp.Error)
		assert.NotNil(t, gf)
		itemID = gf.ID
		itemSize = gf.Size
	}

	return &service.RestorationObject{
		Identifier:             itemIdentifier,
		ItemID:                 itemID,
		BagItProfileIdentifier: profile,
		ObjectSize:             itemSize,
		RestorationSource:      constants.RestorationSourceS3,
		RestorationTarget:      "aptrust.restore.test.test.edu",
		RestorationType:        constants.RestorationTypeObject,
	}
}

func TestNewBagRestorer(t *testing.T) {
	item := itemsToRestore[0]
	restorer := restoration.NewBagRestorer(
		common.NewContext(),
		item.WorkItemID,
		getRestorationObject(t, item.ObjIdentifier, constants.RestorationTypeObject))
	require.NotNil(t, restorer)
	require.NotNil(t, restorer.Context)
	assert.Equal(t, item.WorkItemID, restorer.WorkItemID)
	assert.Equal(t, item.ObjIdentifier, restorer.RestorationObject.Identifier)
}

func TestBagRestorer_Run(t *testing.T) {
	context := common.NewContext()
	setup(t, context)
	for _, item := range itemsToRestore {
		restObj := getRestorationObject(t, item.ObjIdentifier, constants.RestorationTypeObject)
		restorer := restoration.NewBagRestorer(context, item.WorkItemID, restObj)
		fileCount, errors := restorer.Run()
		assert.True(t, fileCount >= 3, fileCount)
		require.Empty(t, errors)
		testRestoredBag(t, context, item)
		testBestRestorationSource(t, restorer)
		testCleanup(t, restorer)
		testRestorationURL(t, restObj)
	}
}

func getIngestObject(objIdentifier string) *service.IngestObject {
	return &service.IngestObject{
		Institution: "test.edu",
		S3Bucket:    "aptrust.restore.test.test.edu",
		S3Key:       objIdentifier + ".tar",
	}
}

func testRestoredBag(t *testing.T, context *common.Context, item RestorationItem) {
	ingestObj := getIngestObject(item.ObjIdentifier)
	m := ingest.NewMetadataGatherer(context, item.WorkItemID, ingestObj)
	fileCount, errors := m.Run()
	assert.Empty(t, errors)

	// fileCount is count of all files in bag, including manifests.
	// APTrust bag has two fewer manifests, one extra tag file: aptrust-info.txt.
	expectedFiles := expectedAPTrustFiles
	if item.ObjIdentifier == btrObject {
		expectedFiles = expectedBTRFiles
	}

	assert.Equal(t, len(expectedFiles), fileCount)

	// Quick spot check: make sure tag rewrite occurred.
	// We test the content of these rewrites elsewhere.
	foundOriginal := false
	foundReplacement := false
	for _, tag := range m.IngestObject.Tags {
		if tag.TagName == "Original-Payload-Oxum" {
			foundOriginal = true
		} else if tag.TagName == "Payload-Oxum" {
			foundReplacement = true
		}
	}
	assert.True(t, foundOriginal, "bag-info.txt is missing Original-Payload-Oxum")
	assert.True(t, foundReplacement, "bag-info.txt is missing replacement Payload-Oxum")

	// Validate the bag
	v := ingest.NewMetadataValidator(context, item.WorkItemID, ingestObj)
	fileCount, errors = v.Run()
	assert.Equal(t, len(expectedFiles), fileCount)
	assert.Empty(t, errors)

	// Do a sanity check on the files. Although the bag may be valid,
	// we still have to ensure that it actually does include the
	// expected files.
	testExpectedFiles(t, context, item, expectedFiles)
}

func testExpectedFiles(t *testing.T, context *common.Context, item RestorationItem, expectedFiles []string) {
	for _, file := range expectedFiles {
		// File identifier has this weird format because we
		// read the object from aptrust.restore.test.edu/test.edu.
		// Normally, the institution identifier appears only once
		// as a prefix, since it's inst/s3_key_name.
		identifier := fmt.Sprintf("test.edu/test.edu/%s", file)
		_, err := context.RedisClient.IngestFileGet(item.WorkItemID, identifier)
		assert.Nil(t, err, "Missing file %s", file)
	}
}

func testBestRestorationSource(t *testing.T, r *restoration.BagRestorer) {
	gf := &registry.GenericFile{
		StorageRecords: []*registry.StorageRecord{
			{URL: "https://s3.us-east-1.localhost:9899/preservation-va/file.txt"},
			{URL: "https://s3.us-west-2.localhost:9899/preservation-or/file.txt"},
		},
	}
	preservationBucket, _, err := restoration.BestRestorationSource(r.Context, gf)
	require.Nil(t, err)
	assert.Equal(t, constants.RegionAWSUSEast1, preservationBucket.Region)
}

func testCleanup(t *testing.T, r *restoration.BagRestorer) {
	for _, alg := range constants.SupportedManifestAlgorithms {
		for _, manifestType := range constants.ManifestTypes {
			manifestFile := r.GetManifestPath(alg, manifestType)
			assert.False(t, util.FileExists(manifestFile))
		}
	}
}

func testRestorationURL(t *testing.T, restObj *service.RestorationObject) {
	expectedURL := fmt.Sprintf("%s%s/%s.tar", constants.AWSBucketPrefix, restObj.RestorationTarget, restObj.Identifier)
	assert.Equal(t, expectedURL, restObj.URL)
}

func TestRewriteTags(t *testing.T) {
	tags := []*bagit.Tag{
		{
			TagFile: "bag-info.txt",
			TagName: "Payload-Oxum",
			Value:   "1234.5",
		},
		{
			TagFile: "bag-info.txt",
			TagName: "Bagging-Date",
			Value:   "2022-04-26",
		},
		{
			TagFile: "bag-info.txt",
			TagName: "Bagging-Software",
			Value:   "The Legend of Baggy Pants",
		},
		{
			TagFile: "bag-info.txt",
			TagName: "Bag-Size",
			Value:   "1.04 K",
		},
		{
			TagFile: "bag-info.txt",
			TagName: "Spongebob",
			Value:   "Squarepants",
		},
	}
	newTags := restoration.RewriteTags(tags, 345000, 12)
	readSeeker, size := restoration.TagsToReadSeeker(newTags)
	assert.EqualValues(t, 311, size)
	buf := new(strings.Builder)
	_, err := io.Copy(buf, readSeeker)
	require.Nil(t, err)
	str := buf.String()
	assert.Contains(t, str, "Payload-Oxum: 345000.12")
	assert.Contains(t, str, "Original-Payload-Oxum: 1234.5")
	assert.Contains(t, str, "Bagging-Date: ")
	assert.Contains(t, str, "Original-Bagging-Date: 2022-04-26")
	assert.Contains(t, str, "Bagging-Software: APTrust preservation-services restoration bagger")
	assert.Contains(t, str, "Original-Bagging-Software: The Legend of Baggy Pants")
	assert.Contains(t, str, "Bag-Size: 336.91 KB")
	assert.Contains(t, str, "Original-Bag-Size: 1.04 K")
}
