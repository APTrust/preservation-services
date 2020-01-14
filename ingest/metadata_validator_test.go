package ingest_test

import (
	"fmt"
	"github.com/APTrust/preservation-services/bagit"
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
	//keysDeleted, err := context.RedisClient.WorkItemDelete(9999)
	//require.Nil(t, err)
	//require.EqualValues(t, 1, keysDeleted)

	// Scan the bag, so that Redis contains the records that the
	// validator needs to read.
	g := ingest.NewMetadataGatherer(context, 9999, validator.IngestObject)
	err := g.ScanBag()
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
	// Default IngestObject has required md5 and sha256
	validator := setupValidator(t)
	assert.True(t, validator.ManifestsRequiredOk())

	// Make the profile require one manifest that's not in the bag
	validator.Profile.ManifestsRequired = []string{"sha512"}
	assert.False(t, validator.ManifestsRequiredOk())
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "Bag is missing required manifest 'sha512'", validator.Errors[0])
}

func TestTagFilesAllowedOk(t *testing.T) {
	// Default profile says any tag files are allowed
	validator := setupValidator(t)
	assert.True(t, validator.TagFilesAllowedOk())

	// Now let's say only one tag file is allowed.
	validator.Profile.TagFilesAllowed = []string{"mytagfile.txt"}
	assert.False(t, validator.TagFilesAllowedOk())
	require.Equal(t, 8, len(validator.Errors))
	for i, filename := range goodbagTagFiles {
		errMsg := fmt.Sprintf("Bag contains illegal tag file '%s'", filename)
		assert.Equal(t, errMsg, validator.Errors[i])
	}
}

func TestTagManifestsAllowedOk(t *testing.T) {
	// Default APTrust profile says md5 and sha256 are allowed.
	// IngestObject has md5 and sha256
	validator := setupValidator(t)
	assert.True(t, validator.TagManifestsAllowedOk())

	// If md5 is not allowed, validation should fail.
	validator.Profile.TagManifestsAllowed = []string{"sha256"}
	assert.False(t, validator.TagManifestsAllowedOk())
	require.Equal(t, 1, len(validator.Errors))
	require.Equal(t, "Bag contains illegal tag manifest 'md5'", validator.Errors[0])
	validator.ClearErrors()

	// Make sure validator reports all errors
	validator.Profile.TagManifestsAllowed = []string{"sha512"}
	assert.False(t, validator.TagManifestsAllowedOk())
	require.Equal(t, 2, len(validator.Errors))
	require.Equal(t, "Bag contains illegal tag manifest 'md5'", validator.Errors[0])
	require.Equal(t, "Bag contains illegal tag manifest 'sha256'", validator.Errors[1])
	validator.ClearErrors()

	// If bag has only some of the allowed manifests, that's OK.
	validator.Profile.TagManifestsAllowed = []string{"sha256", "md5"}
	assert.True(t, validator.TagManifestsAllowedOk())
	require.Equal(t, 0, len(validator.Errors))
}

func TestTagManifestsRequiredOk(t *testing.T) {
	// Default IngestObject has required md5 and sha256
	validator := setupValidator(t)
	assert.True(t, validator.TagManifestsRequiredOk())

	// Make the profile require one manifest that's not in the bag
	validator.Profile.TagManifestsRequired = []string{"sha512"}
	assert.False(t, validator.TagManifestsRequiredOk())
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "Bag is missing required tag manifest 'sha512'", validator.Errors[0])
}

func TestHasAllRequiredTags(t *testing.T) {
	// Default IngestObject has all required tags
	validator := setupValidator(t)
	assert.True(t, validator.HasAllRequiredTags())

	// Add a new requirement & be sure we catch that it's missing.
	tagDef := &bagit.TagDefinition{
		TagFile:  "aptrust-info.txt",
		TagName:  "New-Tag",
		Required: true,
	}
	validator.Profile.Tags = append(validator.Profile.Tags, tagDef)
	assert.False(t, validator.HasAllRequiredTags())
	require.Equal(t, 1, len(validator.Errors))
	require.Equal(t, "Required tag New-Tag in file aptrust-info.txt is missing", validator.Errors[0])
}

func TestExistingTagsOk(t *testing.T) {
	// Default IngestObject has all required tags with valid values
	validator := setupValidator(t)
	assert.True(t, validator.ExistingTagsOk())

	// Empty required tag should cause an error
	titleTag := validator.IngestObject.GetTag("aptrust-info.txt", "Title")
	titleTag.Value = ""

	// Tag with illegal value should also cause an error
	accessTag := validator.IngestObject.GetTag("aptrust-info.txt", "Access")
	accessTag.Value = "semi-private"

	assert.False(t, validator.ExistingTagsOk())
	require.Equal(t, 2, len(validator.Errors))
	assert.Equal(t, "In file aptrust-info.txt, required tag Title has no value", validator.Errors[0])
	assert.Equal(t, "In file aptrust-info.txt, tag Access has illegal value 'semi-private'", validator.Errors[1])
}

func TestIngestFileOk(t *testing.T) {

}

func TestAnythingGoes(t *testing.T) {

}

// TODO: Test IsValid for all valid and invalid APTrust bags
//       Test IsValid for all valid and invalid BTR bags
//       Do this in a separate test file
