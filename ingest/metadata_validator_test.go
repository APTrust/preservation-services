package ingest_test

import (
	"github.com/APTrust/preservation-services/constants"
	//"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/ingest"
	//"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func setupValidator(t *testing.T) *ingest.MetadataValidator {
	// Create a validator
	validator := getMetadataValidator(t,
		constants.BagItProfileDefault, pathToGoodBag, goodbagMd5)

	context := validator.Context

	// Get rid of any stray S3 files from prior test runs
	// and make sure the bag we want to work with is in the
	// local S3 server.
	setupS3(t, context, keyToGoodBag, pathToGoodBag)

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

func TestNewMetadataValidator(t *testing.T) {
	validator := getMetadataValidator(t,
		constants.BagItProfileDefault, pathToGoodBag, goodbagMd5)
	assert.Equal(t, "test", validator.Context.Config.ConfigName)
	assert.Equal(t, "https://wiki.aptrust.org/APTrust_BagIt_Profile-2.2",
		validator.Profile.BagItProfileInfo.BagItProfileIdentifier)
	assert.Equal(t, goodbagMd5, validator.IngestObject.ETag)
	assert.Equal(t, keyToGoodBag, validator.IngestObject.S3Key)
}

func TestBagItVersionOk(t *testing.T) {
	validator := setupValidator(t)

	// In this bag, the version is 0.97, which is permitted under
	// the APTrust profile.
	ok := validator.BagItVersionOk()
	assert.True(t, ok)

	// If version 0.97 is not in AcceptBagItVersion, make sure
	// it's rejected with a suitable message.
	validator.Profile.AcceptBagItVersion = []string{"1.0", "1.1"}
	ok = validator.BagItVersionOk()
	assert.False(t, ok)
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "BagIt-Version 0.97 is not permitted in BagIt profile https://wiki.aptrust.org/APTrust_BagIt_Profile-2.2.", validator.Errors[0])

	// Clear the errors, and make sure we get a desciptive error
	// for empty BagIt-Version tag.
	validator.Errors = make([]string, 0)
	tag := validator.IngestObject.GetTag("bagit.txt", "BagIt-Version")
	tag.Value = ""
	ok = validator.BagItVersionOk()
	assert.False(t, ok)
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "Missing required tag bag-info.txt/BagIt-Version.", validator.Errors[0])

	// Should get same error if tag is entirely missing
	validator.Errors = make([]string, 0)
	tag = nil
	ok = validator.BagItVersionOk()
	assert.False(t, ok)
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "Missing required tag bag-info.txt/BagIt-Version.", validator.Errors[0])
}

func TestSerializationOk(t *testing.T) {

}

func TestFetchTxtOk(t *testing.T) {

}

func TestManifestsAllowedOk(t *testing.T) {

}

func TestManifestsRequiredOk(t *testing.T) {

}

func TagFilesAllowedOk(t *testing.T) {

}

func TestTagManifestsAllowedOk(t *testing.T) {

}

func TestTagManifestsAllowedRequiredOk(t *testing.T) {

}

func TestHasAllRequiredTags(t *testing.T) {

}

func TestExistingTagsOk(t *testing.T) {

}

func TestTagOk(t *testing.T) {

}

func TestIngestFileOk(t *testing.T) {

}

func TestAddError(t *testing.T) {

}

func TestAnythingGoes(t *testing.T) {

}

func TestValidateAllowed(t *testing.T) {

}

func TestValidateRequired(t *testing.T) {

}

func TestRecordIllegals(t *testing.T) {

}

// TODO: Test IsValid for all valid and invalid APTrust bags
//       Test IsValid for all valid and invalid BTR bags
//       Do this in a separate test file
