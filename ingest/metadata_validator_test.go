package ingest_test

import (
	//"fmt"
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
	validator.ClearErrors()
	tag := validator.IngestObject.GetTag("bagit.txt", "BagIt-Version")
	tag.Value = ""
	ok = validator.BagItVersionOk()
	assert.False(t, ok)
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "Missing required tag bag-info.txt/BagIt-Version.", validator.Errors[0])

	// Should get same error if tag is entirely missing
	validator.ClearErrors()
	tag = nil
	ok = validator.BagItVersionOk()
	assert.False(t, ok)
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "Missing required tag bag-info.txt/BagIt-Version.", validator.Errors[0])
}

func TestSerializationOk(t *testing.T) {
	// Default obj has format "application/tar" and default
	// profile says serialization is required in format
	// "application/tar", so this should be OK.
	validator := setupValidator(t)
	assert.True(t, validator.SerializationOk())

	// Serialization not OK if serialization is forbidden.
	validator.Profile.Serialization = "forbidden"
	assert.False(t, validator.SerializationOk())
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "BagIt profile forbids serialization but bag is serialized in application/tar format", validator.Errors[0])
	validator.ClearErrors()

	// Not OK if profile does not accept tar format,
	// regardless of Serialization value.
	//
	// 1. Required, but format not allowed.
	validator.Profile.Serialization = "required"
	validator.Profile.AcceptSerialization = []string{}
	assert.False(t, validator.SerializationOk())
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "BagIt profile does not allow serialization format application/tar", validator.Errors[0])
	validator.ClearErrors()

	// 2. Optional, but format not allowed
	validator.Profile.Serialization = "optional"
	assert.False(t, validator.SerializationOk())
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "BagIt profile does not allow serialization format application/tar", validator.Errors[0])
	validator.ClearErrors()

	// 3. Forbidden, but bag is serialized.
	validator.Profile.Serialization = "forbidden"
	assert.False(t, validator.SerializationOk())
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "BagIt profile forbids serialization but bag is serialized in application/tar format", validator.Errors[0])
	validator.ClearErrors()

	// OK if no serialization in the following two cases.
	validator.IngestObject.Serialization = ""
	validator.Profile.Serialization = "forbidden"
	assert.True(t, validator.SerializationOk())
	validator.Profile.Serialization = "optional"
	assert.True(t, validator.SerializationOk())

	// Not OK if required but unserialized.
	validator.Profile.Serialization = "required"
	validator.Profile.AcceptSerialization = []string{"application/tar", "application/gzip"}
	assert.False(t, validator.SerializationOk())
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "Bag is not serialized, but profile requires serialization in one of the following formats: application/tar, application/gzip", validator.Errors[0])
	validator.ClearErrors()
}

func TestFetchTxtOk(t *testing.T) {
	// In default case, profile says fetch.txt is not
	// allowed, and IngestObject says it's not part
	// of the bag.
	validator := setupValidator(t)
	assert.True(t, validator.FetchTxtOk())

	// Not OK: profile says not allowed, but file is present.
	validator.IngestObject.HasFetchTxt = true
	assert.False(t, validator.FetchTxtOk())
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "Bag has fetch.txt file which profile does not allow", validator.Errors[0])
	validator.ClearErrors()

	// OK: profile says it's allowed & file is present.
	validator.Profile.AllowFetchTxt = true
	assert.True(t, validator.FetchTxtOk())

	// OK: profile says it's allowed, but file is not present.
	validator.IngestObject.HasFetchTxt = false
	assert.True(t, validator.FetchTxtOk())
}

func TestManifestsAllowedOk(t *testing.T) {
	// Default APTrust profile says md5 and sha256 are allowed.
	// IngestObject has md5 and sha256
	validator := setupValidator(t)
	assert.True(t, validator.ManifestsAllowedOk())

	// If md5 is not allowed, validation should fail.
	validator.Profile.ManifestsAllowed = []string{"sha256"}
	assert.False(t, validator.ManifestsAllowedOk())
	require.Equal(t, 1, len(validator.Errors))
	require.Equal(t, "Bag contains illegal manifest 'md5'", validator.Errors[0])
	validator.ClearErrors()

	// Make sure validator reports all errors
	validator.Profile.ManifestsAllowed = []string{"sha512"}
	assert.False(t, validator.ManifestsAllowedOk())
	require.Equal(t, 2, len(validator.Errors))
	require.Equal(t, "Bag contains illegal manifest 'md5'", validator.Errors[0])
	require.Equal(t, "Bag contains illegal manifest 'sha256'", validator.Errors[1])
	validator.ClearErrors()

	// If bag has only some of the allowed manifests, that's OK.
	validator.Profile.ManifestsAllowed = []string{"sha256", "md5"}
	assert.True(t, validator.ManifestsAllowedOk())
	require.Equal(t, 0, len(validator.Errors))
}

func TestManifestsRequiredOk(t *testing.T) {
	// START HERE
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

func TestClearErrors(t *testing.T) {

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
