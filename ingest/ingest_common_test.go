package ingest_test

import (
	"fmt"
	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path"
	"path/filepath"
	"testing"
)

var keyToGoodBag = "example.edu.tagsample_good.tar"
var pathToGoodBag = testutil.PathToUnitTestBag(keyToGoodBag)
var goodbagMd5 = "f4323e5e631834c50d077fc3e03c2fed"
var goodbagSize = int64(40960)
var goodbagS3Files = []string{
	"aptrust-info.txt",
	"bag-info.txt",
	"bagit.txt",
	"manifest-md5.txt",
	"manifest-sha256.txt",
	"tagmanifest-md5.txt",
	"tagmanifest-sha256.txt",
}
var goodbags3FileSizes = []int64{
	int64(67),
	int64(297),
	int64(55),
	int64(230),
	int64(358),
	int64(438),
	int64(694),
}
var goodbagOtherFiles = []string{
	"data/datastream-DC",
	"data/datastream-descMetadata",
	"data/datastream-MARC",
	"data/datastream-RELS-EXT",
	"custom_tags/tracked_file_custom.xml",
	"custom_tags/tracked_tag_file.txt",
	"custom_tags/untracked_tag_file.txt",
}

var goodbagTagFiles = []string{
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
func setupS3(t *testing.T, context *common.Context, key, pathToBagFile string) {
	clearS3Files(t, context)
	putBagInS3(t, context, key, pathToBagFile)
}

// Get rid of text files that may be lingering in our local
// in-memory S3 server from the previous test.
func clearS3Files(t *testing.T, context *common.Context) {
	for _, filename := range goodbagS3Files {
		key := fmt.Sprintf("9999/%s", filename)
		_ = context.S3Clients[constants.S3ClientAWS].RemoveObject(
			constants.TestBucketReceiving,
			key)
		//require.Nil(t, err)
	}
}

// Copy goodbag to local in-memory S3 service.
func putBagInS3(t *testing.T, context *common.Context, key, pathToBagFile string) {
	// Uncomment the following to get a full printout
	// of the client's HTTP exchanges on Stderr.
	//context.S3Clients[constants.S3ClientAWS].TraceOn(os.Stderr)

	bytesWritten, err := context.S3Clients[constants.S3ClientAWS].FPutObject(
		constants.TestBucketReceiving,
		key,
		pathToBagFile,
		minio.PutObjectOptions{})
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	require.Nil(t, err, msg)
	assert.True(t, (bytesWritten >= goodbagSize))
}

func deleteChecksum(list []*service.IngestChecksum, source, algorithm string) []*service.IngestChecksum {
	checksums := make([]*service.IngestChecksum, 0)
	for _, cs := range list {
		if cs.Source == source && cs.Algorithm == algorithm {
			continue
		} else {
			checksums = append(checksums, cs)
		}
	}
	return checksums
}

// Returns an IngestObject that describes the tarred bag waiting
// in our receiving bucket.
func getIngestObject(pathToBagFile, md5Digest string) *service.IngestObject {
	obj := service.NewIngestObject(
		constants.TestBucketReceiving, // bucket
		filepath.Base(pathToBagFile),  // key
		md5Digest,                     // eTag
		"example.edu",                 // institution
		goodbagSize,                   // size
	)
	obj.Serialization = "application/tar"
	return obj
}

// Valid names are constants.BagItProfileBTR and constant.BagItProfileDefault
func getProfile(name string) (*bagit.BagItProfile, error) {
	filename := path.Join(testutil.ProjectRoot(), "profiles", "aptrust-v2.2.json")
	return bagit.BagItProfileLoad(filename)
}

func getMetadataValidator(t *testing.T, profileName, pathToBag, bagMd5 string) *ingest.MetadataValidator {
	context := common.NewContext()
	profile, err := getProfile(profileName)
	require.Nil(t, err)
	require.NotNil(t, profile)
	obj := getIngestObject(pathToBag, bagMd5)
	validator := ingest.NewMetadataValidator(context, profile, obj, 9999)
	require.NotNil(t, validator)
	return validator
}

func setupValidatorAndObject(t *testing.T, profileName, pathToBag, bagMd5 string) *ingest.MetadataValidator {
	// Create a validator
	validator := getMetadataValidator(t, profileName, pathToBag, bagMd5)
	context := validator.Context

	// Get rid of any stray S3 files from prior test runs
	// and make sure the bag we want to work with is in the
	// local S3 server.
	key := filepath.Base(pathToBag)
	setupS3(t, context, key, pathToBag)

	// Get rid of old redis records related to this bag / work item
	keysDeleted, err := context.RedisClient.WorkItemDelete(9999)
	require.Nil(t, err)
	require.EqualValues(t, 1, keysDeleted)

	// Scan the bag, so that Redis contains the records that the
	// validator needs to read.
	g := ingest.NewMetadataGatherer(context, 9999, validator.IngestObject)
	err = g.ScanBag()
	require.Nil(t, err)

	return validator
}
