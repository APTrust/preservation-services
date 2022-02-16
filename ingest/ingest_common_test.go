package ingest_test

import (
	ctx "context"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
)

// WorkItem ID for StagingUploader tests, so we don't conflict with ids
// used in other tests inside of ingest_test
const testWorkItemId = 77977

var keyToGoodBag = "example.edu.tagsample_good.tar"
var pathToGoodBag = testutil.PathToUnitTestBag(keyToGoodBag)
var goodbagMd5 = "f4323e5e631834c50d077fc3e03c2fed"
var goodbagSize = int64(40960)

var keyToBadBag = "example.edu.tagsample_bad.tar"
var pathToBadBag = testutil.PathToUnitTestBag(keyToBadBag)
var badbagMd5 = "d3def62b6d46744eb44fedd7239752ff"
var badbagSize = int64(32768)

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
		_ = context.S3Clients[constants.StorageProviderAWS].RemoveObject(
			ctx.Background(),
			constants.TestBucketReceiving,
			key,
			minio.RemoveObjectOptions{})
		//require.Nil(t, err)
	}
}

// Copy goodbag to local in-memory S3 service.
func putBagInS3(t *testing.T, context *common.Context, key, pathToBagFile string) {
	// Uncomment the following to get a full printout
	// of the client's HTTP exchanges on Stderr.
	//context.S3Clients[constants.StorageProviderAWS].TraceOn(os.Stderr)

	_, err := context.S3Clients[constants.StorageProviderAWS].FPutObject(
		ctx.Background(),
		constants.TestBucketReceiving,
		key,
		pathToBagFile,
		minio.PutObjectOptions{})
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	require.Nil(t, err, msg)
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

func getInstFromFileName(pathToBagFile string) string {
	base := path.Base(pathToBagFile)
	parts := strings.Split(base, ".")
	return strings.Join(parts[0:2], ".")
}

// Returns an IngestObject that describes the tarred bag waiting
// in our receiving bucket.
func getIngestObject(pathToBagFile, md5Digest string) *service.IngestObject {
	inst := getInstFromFileName(pathToBagFile)
	obj := service.NewIngestObject(
		constants.TestBucketReceiving, // bucket
		filepath.Base(pathToBagFile),  // key
		md5Digest,                     // eTag
		inst,                          // institution
		9855,                          // institution id
		goodbagSize,                   // size
	)
	obj.Serialization = "application/tar"
	return obj
}

// Valid names are constants.BagItProfileBTR and constant.BagItProfileDefault
func getProfile(name string) (*bagit.Profile, error) {
	filename := path.Join(util.ProjectRoot(), "profiles", name)
	return bagit.ProfileLoad(filename)
}

func getMetadataValidator(t *testing.T, profileName, pathToBag, bagMd5 string, workItemId int64) *ingest.MetadataValidator {
	context := common.NewContext()
	profile, err := getProfile(profileName)
	require.Nil(t, err)
	require.NotNil(t, profile)
	obj := getIngestObject(pathToBag, bagMd5)
	validator := ingest.NewMetadataValidator(context, workItemId, obj)
	require.NotNil(t, validator)
	validator.Profile = profile
	return validator
}

// This function prepares a bag for validation by running it through
// the MetadataGatherer. It returns a MetadataValidator that is ready
// to be tested.
func setupValidatorAndObject(t *testing.T, profileName, pathToBag, bagMd5 string, workItemId int64, testForScanError bool) *ingest.MetadataValidator {
	// Create a validator
	validator := getMetadataValidator(t, profileName, pathToBag, bagMd5, workItemId)
	context := validator.Context

	// Get rid of any stray S3 files from prior test runs
	// and make sure the bag we want to work with is in the
	// local S3 server.
	key := filepath.Base(pathToBag)
	setupS3(t, context, key, pathToBag)

	// Get rid of old redis records related to this bag / work item
	_, err := context.RedisClient.WorkItemDelete(workItemId)
	require.Nil(t, err)
	//require.EqualValues(t, 1, keysDeleted)

	// Scan the bag, so that Redis contains the records that the
	// validator needs to read.
	g := ingest.NewMetadataGatherer(context, workItemId, validator.IngestObject)
	_, errors := g.Run()

	// Most tests do no produce a bag scanning error, but one does:
	// bag_validation_test.go / TestBag_SampleWrongFolderName.
	// In that case we want to ensure the error gets passed back through
	// the validator. In all other cases, a scan error will prevent
	// the rest of the tests from running, so we want to fail early in
	// those cases.
	if testForScanError {
		require.Empty(t, errors)
	}

	return validator
}

// This function does the prep work required to test the StagingUploader.
// It sends our test bag through all stages of ingest prior to the
// staging upload. Note that this ensures proper S3 setup and deletes
// Redis records related to our WorkItem so that we start with a fresh
// slate each time.
func prepareForCopyToStaging(t *testing.T, pathToBag string, workItemId int64, context *common.Context) *ingest.StagingUploader {
	// Put tagsample_good in S3 receiving bucket.
	setupS3(t, context, path.Base(pathToBag), pathToBag)

	// Get rid of old redis records related to this bag / work item
	_, err := context.RedisClient.WorkItemDelete(workItemId)
	require.Nil(t, err)

	// Set up an ingest object, and assign the correct institution id.
	// We can't know this id ahead of time because of the way Registry
	// loads fixture data.
	instIdentifier := getInstFromFileName(pathToBag)
	obj := getIngestObject(pathToBag, goodbagMd5)
	inst := context.RegistryClient.InstitutionByIdentifier(instIdentifier).Institution()
	require.NotNil(t, inst)
	obj.InstitutionID = inst.ID

	err = context.RedisClient.IngestObjectSave(workItemId, obj)
	require.Nil(t, err)

	// Scan and validate the bag, so Redis has all the expected data.
	gatherer := ingest.NewMetadataGatherer(context, workItemId, obj)
	_, errors := gatherer.Run()
	require.Empty(t, errors)

	// Validate the bag.
	filename := path.Join(util.ProjectRoot(), "profiles", "aptrust-v2.2.json")
	profile, err := bagit.ProfileLoad(filename)
	require.Nil(t, err)
	validator := ingest.NewMetadataValidator(context, workItemId, obj)
	validator.Profile = profile
	require.True(t, validator.IsValid())

	// Check for reingest
	reingestManager := ingest.NewReingestManager(context, workItemId, obj)
	_, errors = reingestManager.Run()
	require.Empty(t, errors)

	return ingest.NewStagingUploader(context, workItemId, obj)
}

// This lays the groundwork to test the PreservationUploader. It pushes our
// test bag through all phases of ingest prior to preservation upload.
func prepareForPreservationUpload(t *testing.T, pathToBag string, workItemId int64, context *common.Context) *ingest.PreservationUploader {
	uploader := prepareForCopyToStaging(t, pathToBag, workItemId, context)
	_, errors := uploader.Run()
	require.Empty(t, errors)

	fi := ingest.NewFormatIdentifier(context, uploader.WorkItemID, uploader.IngestObject)
	_, errors = fi.Run()
	require.Empty(t, errors)
	//assert.Equal(t, 16, numberIdentified)
	return ingest.NewPreservationUploader(context, workItemId, uploader.IngestObject)
}

func prepareForPreservationVerify(t *testing.T, pathToBag string, workItemId int64, context *common.Context) *ingest.PreservationVerifier {
	uploader := prepareForPreservationUpload(t, pathToBag, workItemId, context)
	_, errors := uploader.Run()
	require.Empty(t, errors, errors)
	return ingest.NewPreservationVerifier(context, uploader.WorkItemID, uploader.IngestObject)
}

func prepareForRecord(t *testing.T, pathToBag string, workItemId int64, context *common.Context) *ingest.Recorder {
	verifier := prepareForPreservationVerify(t, pathToBag, workItemId, context)
	_, errors := verifier.Run()
	require.Empty(t, errors, errors)
	return ingest.NewRecorder(context, verifier.WorkItemID, verifier.IngestObject)
}

func prepareForCleanup(t *testing.T, pathToBag string, workItemId int64, context *common.Context) *ingest.Cleanup {
	recorder := prepareForRecord(t, pathToBag, workItemId, context)
	_, errors := recorder.Run()
	require.Empty(t, errors, errors)
	return ingest.NewCleanup(context, recorder.WorkItemID, recorder.IngestObject)
}
