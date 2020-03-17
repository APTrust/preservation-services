// +build integration

package ingest_test

import (
	//	"fmt"
	"github.com/APTrust/preservation-services/bagit"
	//	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	//	"github.com/APTrust/preservation-services/models/registry"
	//	"github.com/APTrust/preservation-services/models/service"
	//	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/testutil"
	// "github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path"
	//	"strings"
	"testing"
	//	"time"
)

const tarHeaderName = "example.edu.tagsample_good/data/datastream-descMetadata"
const filePathInBag = "data/datastream-descMetadata"
const objectIdentifier = "example.edu/example.edu.tagsample_good"
const fileIdentifier = "example.edu/example.edu.tagsample_good/data/datastream-descMetadata"

func prepareForCopyToStaging(t *testing.T, context *common.Context) *ingest.StagingUploader {
	// Put tagsample_good in S3 receiving bucket.
	setupS3(t, context, keyToGoodBag, pathToGoodBag)

	// Set up an ingest object, and assign the correct institution id.
	// We can't know this id ahead of time because of the way Pharos
	// loads fixture data.
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	inst := context.PharosClient.InstitutionGet("example.edu").Institution()
	require.NotNil(t, inst)
	obj.InstitutionId = inst.Id

	// Scan and validate the bag, so Redis has all the expected data.
	gatherer := ingest.NewMetadataGatherer(context, 9999, obj)
	err := gatherer.ScanBag()
	require.Nil(t, err)

	// Validate the bag.
	filename := path.Join(testutil.ProjectRoot(), "profiles", "aptrust-v2.2.json")
	profile, err := bagit.BagItProfileLoad(filename)
	require.Nil(t, err)
	validator := ingest.NewMetadataValidator(context, profile, obj, 9999)
	require.True(t, validator.IsValid())

	return ingest.NewStagingUploader(context, 9999, obj)
}

func TestNewStagingUploader(t *testing.T) {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	uploader := ingest.NewStagingUploader(context, 9999, obj)
	require.NotNil(t, uploader)
	assert.Equal(t, context, uploader.Context)
	assert.Equal(t, 9999, uploader.WorkItemId)
	assert.Equal(t, obj, uploader.IngestObject)
}

func TestStagingUploader_GetS3Object(t *testing.T) {
	context := common.NewContext()
	setupS3(t, context, keyToGoodBag, pathToGoodBag)
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	uploader := ingest.NewStagingUploader(context, 9999, obj)
	s3Obj, err := uploader.GetS3Object()
	require.Nil(t, err)
	require.NotNil(t, s3Obj)
}

func TestCopyFilesToStaging(t *testing.T) {
	context := common.NewContext()
	uploader := prepareForCopyToStaging(t, context)

	testIdentifierGetFileAndPutOptions(t, uploader)
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

	// Test GetPutOptions
	opts, err := uploader.GetPutOptions(ingestFile)
	require.Nil(t, err)
	assert.Equal(t, "example.edu", opts.UserMetadata["institution"])
	assert.Equal(t, objectIdentifier, opts.UserMetadata["bag"])
	assert.Equal(t, filePathInBag, opts.UserMetadata["bagpath"])
	assert.Equal(t, "4bd0ad5f85c00ce84a455466b24c8960", opts.UserMetadata["md5"])
	assert.Equal(t, "cf9cbce80062932e10ee9cd70ec05ebc24019deddfea4e54b8788decd28b4bc7", opts.UserMetadata["sha256"])
}

// func testMarkFileAsCopied(t *testing.T, uploader *ingest.StagingUploader) {

// }

// func testGetIngestFile(t *testing.T, uploader *ingest.StagingUploader) {
// 	ingestFile, err := uploader.GetIngestFile(filePathInBag)
// 	assert.Nil(t, err)
// 	require.NotNil(t, ingestFile)
// 	assert.Equal(t, fileIdentifier, ingestFile)
// }

// func testGetGenericFileIdentifier(t *testing.T, uploader *ingest.StagingUploader) {
// 	assert.Equal(t,
// 		fileIdentifier,
// 		uploader.GetGenericFileIdentifier(filePathInBag))
// }
