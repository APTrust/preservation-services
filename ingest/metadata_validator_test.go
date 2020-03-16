package ingest_test

import (
	"fmt"
	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNewMetadataValidator(t *testing.T) {
	validator := getMetadataValidator(t,
		constants.BagItProfileDefault, pathToGoodBag, goodbagMd5)
	assert.Equal(t, "test", validator.Context.Config.ConfigName)
	assert.Equal(t, constants.DefaultProfileIdentifier,
		validator.Profile.BagItProfileInfo.BagItProfileIdentifier)
	assert.Equal(t, goodbagMd5, validator.IngestObject.ETag)
	assert.Equal(t, keyToGoodBag, validator.IngestObject.S3Key)
}

func TestBagItVersionOk(t *testing.T) {
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToGoodBag, goodbagMd5, true)

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
	assert.Equal(t, fmt.Sprintf("BagIt-Version 0.97 is not permitted in BagIt profile %s.", constants.DefaultProfileIdentifier), validator.Errors[0])

	// Clear the errors, and make sure we get a desciptive error
	// for empty BagIt-Version tag.
	validator.ClearErrors()
	tag := validator.IngestObject.GetTag("bagit.txt", "BagIt-Version")
	tag.Value = ""
	ok = validator.BagItVersionOk()
	assert.False(t, ok)
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "Missing required tag bagit.txt/BagIt-Version.", validator.Errors[0])

	// Should get same error if tag is entirely missing
	validator.ClearErrors()
	tag = nil
	ok = validator.BagItVersionOk()
	assert.False(t, ok)
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "Missing required tag bagit.txt/BagIt-Version.", validator.Errors[0])
}

func TestSerializationOk(t *testing.T) {
	// Default obj has format "application/tar" and default
	// profile says serialization is required in format
	// "application/tar", so this should be OK.
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToGoodBag, goodbagMd5, true)
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
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToGoodBag, goodbagMd5, true)
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
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToGoodBag, goodbagMd5, true)
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
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToGoodBag, goodbagMd5, true)
	assert.True(t, validator.ManifestsRequiredOk())

	// Make the profile require one manifest that's not in the bag
	validator.Profile.ManifestsRequired = []string{"sha512"}
	assert.False(t, validator.ManifestsRequiredOk())
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "Bag is missing required manifest 'sha512'", validator.Errors[0])
}

func TestTagFilesAllowedOk(t *testing.T) {
	// Default profile says any tag files are allowed
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToGoodBag, goodbagMd5, true)
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
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToGoodBag, goodbagMd5, true)
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
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToGoodBag, goodbagMd5, true)
	assert.True(t, validator.TagManifestsRequiredOk())

	// Make the profile require one manifest that's not in the bag
	validator.Profile.TagManifestsRequired = []string{"sha512"}
	assert.False(t, validator.TagManifestsRequiredOk())
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "Bag is missing required tag manifest 'sha512'", validator.Errors[0])
}

func TestHasAllRequiredTags(t *testing.T) {
	// Default IngestObject has all required tags
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToGoodBag, goodbagMd5, true)
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
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToGoodBag, goodbagMd5, true)
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

func TestAnythingGoes(t *testing.T) {
	validator := getMetadataValidator(t,
		constants.BagItProfileDefault, pathToGoodBag, goodbagMd5)
	assert.True(t, validator.AnythingGoes(nil))
	assert.True(t, validator.AnythingGoes([]string{}))
	assert.True(t, validator.AnythingGoes([]string{"*"}))
	assert.False(t, validator.AnythingGoes([]string{"sha256"}))
}

// TODO: Test payload files and manifest files separately.
//
// 1. Payload file: with match, with mismatch, manifest missing,
//    file missling, illegal name.
// 2. Non-required tag file: with match, mismatch, missing digest, illegal name.
// 3. Required tag file, manifest: match, mismatch, manifest missing,
//    file missing, illegal name.
func TestIngestFileOk_PayloadFile(t *testing.T) {
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToGoodBag, goodbagMd5, true)

	identifier := fmt.Sprintf("%s/%s",
		validator.IngestObject.Identifier(),
		"data/datastream-descMetadata")
	f, err := validator.Context.RedisClient.IngestFileGet(
		9999, identifier)
	require.Nil(t, err)

	// Force check of manifests.
	assert.True(t, validator.IngestFileOk(f))

	// Force checksum mismatch.
	origDigest := f.Checksums[0].Digest
	f.Checksums[0].Digest = "98765"
	assert.False(t, validator.IngestFileOk(f))

	// Make sure exact error was captured.
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "File example.edu/example.edu.tagsample_good/data/datastream-descMetadata: ingest md5 checksum 98765 doesn't match manifest checksum 4bd0ad5f85c00ce84a455466b24c8960", validator.Errors[0])

	// Fix the digest, so it doesn't interfere with the next test.
	f.Checksums[0].Digest = origDigest

	// If manifest checksum is missing, we should hear about it.
	// Save a copy of the manifest checksum, then delete it from
	// the ingest file object.
	manifestChecksum := f.GetChecksum(
		constants.SourceManifest, constants.AlgSha256)
	f.Checksums = deleteChecksum(f.Checksums,
		constants.SourceManifest, constants.AlgSha256)

	// Clear old errors and re-validate
	validator.Errors = []string{}
	assert.False(t, validator.IngestFileOk(f))

	// Make sure exact error was captured.
	require.Equal(t, 1, len(validator.Errors))
	require.Equal(t, "File example.edu/example.edu.tagsample_good/data/datastream-descMetadata is not in manifest manifest-sha256.txt", validator.Errors[0])

	// Put the manifest checksum back and delete the ingest checksum.
	ingestChecksum := f.GetChecksum(
		constants.SourceIngest, constants.AlgSha256)
	f.Checksums = append(f.Checksums, manifestChecksum)
	f.Checksums = deleteChecksum(f.Checksums,
		constants.SourceIngest, constants.AlgSha256)

	// Clear old errors and re-validate
	validator.Errors = []string{}
	assert.False(t, validator.IngestFileOk(f))

	// Make sure exact error was captured.
	require.Equal(t, 1, len(validator.Errors))
	require.Equal(t, "File example.edu/example.edu.tagsample_good/data/datastream-descMetadata in manifest-sha256.txt is missing from bag", validator.Errors[0])

	// Put the ingest checksum back, so all checksums are present.
	f.Checksums = append(f.Checksums, ingestChecksum)

	// Clear the errors
	validator.Errors = []string{}

	// Give the file an illegal name...
	f.PathInBag = "data/illegal\u0006.txt"

	// Make sure we get a specific error for this.
	assert.False(t, validator.IngestFileOk(f))
	require.Equal(t, 1, len(validator.Errors))
	require.Equal(t, "File name 'data/illegal\x06.txt' contains one or more illegal control characters", validator.Errors[0])
}

// Manifests are required to appear in tagmanifests
func TestIngestFileOk_RequiredTagFile(t *testing.T) {
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToGoodBag, goodbagMd5, true)

	identifier := fmt.Sprintf("%s/%s",
		validator.IngestObject.Identifier(),
		"manifest-md5.txt")
	f, err := validator.Context.RedisClient.IngestFileGet(
		9999, identifier)
	require.Nil(t, err)

	// Force check of tag manifests.
	assert.True(t, validator.IngestFileOk(f))

	// Force checksum mismatch.
	origDigest := f.Checksums[0].Digest
	f.Checksums[0].Digest = "98765"
	assert.False(t, validator.IngestFileOk(f))

	// Make sure exact error was captured.
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "File example.edu/example.edu.tagsample_good/manifest-md5.txt: ingest md5 checksum 98765 doesn't match manifest checksum a541b543dad466a93ab60e785671982b", validator.Errors[0])

	// Fix the digest, so it doesn't interfere with the next test.
	f.Checksums[0].Digest = origDigest

	// If manifest checksum is missing, we should hear about it.
	// Save a copy of the manifest checksum, then delete it from
	// the ingest file object.
	tagManifestChecksum := f.GetChecksum(
		constants.SourceTagManifest, constants.AlgSha256)
	f.Checksums = deleteChecksum(f.Checksums,
		constants.SourceTagManifest, constants.AlgSha256)

	// Clear old errors and re-validate
	validator.Errors = []string{}
	assert.False(t, validator.IngestFileOk(f))

	// Make sure exact error was captured.
	require.Equal(t, 1, len(validator.Errors))
	require.Equal(t, "File example.edu/example.edu.tagsample_good/manifest-md5.txt is not in manifest tagmanifest-sha256.txt", validator.Errors[0])

	// Put the manifest checksum back and delete the ingest checksum.
	f.Checksums = append(f.Checksums, tagManifestChecksum)
	f.Checksums = deleteChecksum(f.Checksums,
		constants.SourceIngest, constants.AlgSha256)

	// Clear old errors and re-validate
	validator.Errors = []string{}
	assert.False(t, validator.IngestFileOk(f))

	// Make sure exact error was captured.
	require.Equal(t, 1, len(validator.Errors))
	require.Equal(t, "File example.edu/example.edu.tagsample_good/manifest-md5.txt in tagmanifest-sha256.txt is missing from bag", validator.Errors[0])
}

// Tag files are not required to appear in tagmanifests
func TestIngestFileOk_NonRequiredTagFile(t *testing.T) {
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToGoodBag, goodbagMd5, true)

	identifier := fmt.Sprintf("%s/%s",
		validator.IngestObject.Identifier(),
		"custom_tags/untracked_tag_file.txt")
	f, err := validator.Context.RedisClient.IngestFileGet(
		9999, identifier)
	require.Nil(t, err)

	// Verify that there's no tag manifest checksum for this file.
	tagManifestChecksum := f.GetChecksum(
		constants.SourceTagManifest, constants.AlgSha256)
	assert.Nil(t, tagManifestChecksum)

	// Run the checksum validation...
	assert.True(t, validator.IngestFileOk(f))

	// ...and not that the missing tag manifest checksum should NOT
	// produce an error, because the tag file doesn't have
	// to appear in the tag manifest.
	require.Equal(t, 0, len(validator.Errors))

	// If we add a tagmanifest checksum that doesn't match what
	// the metadata gatherer calculated on ingest, it is an error.
	f.Checksums = append(f.Checksums,
		&service.IngestChecksum{
			Algorithm: constants.AlgSha256,
			DateTime:  time.Now().UTC(),
			Digest:    "1234",
			Source:    constants.SourceTagManifest,
		})

	// Clear old errors and re-validate
	validator.Errors = []string{}
	assert.False(t, validator.IngestFileOk(f))

	// Make sure exact error was captured.
	// This is an error because if the manifest says it's there,
	// it has to be there.
	require.Equal(t, 1, len(validator.Errors))
	require.Equal(t, "File example.edu/example.edu.tagsample_good/custom_tags/untracked_tag_file.txt: ingest sha256 checksum 1488c68d6d6d839e8f913bc50bf555878b83aa29e027e386bf9eec9462e9f54c doesn't match manifest checksum 1234", validator.Errors[0])

	// This should also produce an error. If the tagmanifest
	// says it's there, it should be there.
	f.Checksums = deleteChecksum(f.Checksums,
		constants.SourceIngest, constants.AlgSha256)

	// Clear old errors and re-validate
	validator.Errors = []string{}
	assert.False(t, validator.IngestFileOk(f))

	// Make sure exact error was captured.
	// This is an error because if the manifest says it's there,
	// it has to be there.
	require.Equal(t, 1, len(validator.Errors))
	require.Equal(t, "File example.edu/example.edu.tagsample_good/custom_tags/untracked_tag_file.txt in tagmanifest-sha256.txt is missing from bag", validator.Errors[0])
}

func TestValidator_IsValid(t *testing.T) {
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToGoodBag, goodbagMd5, true)
	assert.True(t, validator.IsValid())
}
