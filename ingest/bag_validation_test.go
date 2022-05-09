package ingest_test

import (
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Note: In these tests, the last param to setUpValidatorAndObject
// can be an empty string. Ingest code uses the IngestObject md5
// value to ensure it's working on the right version of a bag in
// an ingest bucket. The bag md5 is irrelevant in this context
// and can be anything.

const validationID = 54321

func TestBag_WithFetchTxt(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("example.edu.fetchtxt.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, "", validationID, true)
	assert.False(t, validator.IsValid())
	assert.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "Bag has fetch.txt file which profile does not allow", validator.Errors[0])
}

func TestBag_MultiPart_1(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("example.edu.multipart.b01.of02.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, "", validationID, true)
	assert.True(t, validator.IsValid())
	assert.Equal(t, 0, len(validator.Errors))
}

func TestBag_MultiPart_2(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("example.edu.multipart.b02.of02.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, "", validationID, true)
	assert.True(t, validator.IsValid())
	assert.Equal(t, 0, len(validator.Errors))
}

func TestBag_BadAccess(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("example.edu.sample_bad_access.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, "", validationID, true)
	assert.False(t, validator.IsValid())
	assert.Equal(t, 1, len(validator.Errors))
	assert.Equal(t,
		"In file aptrust-info.txt, tag Access has illegal value 'Hands Off!'",
		validator.Errors[0])
}

func TestBag_BadChecksums(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("example.edu.sample_bad_checksums.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, "", validationID, true)
	assert.False(t, validator.IsValid())
	assert.Equal(t, 1, len(validator.Errors))
}

func TestBag_GlacierOH(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("example.edu.sample_glacier_oh.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, "", validationID, true)
	assert.True(t, validator.IsValid())
	assert.Equal(t, 0, len(validator.Errors))
}

func TestBag_GlacierOR(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("example.edu.sample_glacier_or.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, "", validationID, true)
	assert.True(t, validator.IsValid())
	assert.Equal(t, 0, len(validator.Errors))
}

func TestBag_GlacierVA(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("example.edu.sample_glacier_va.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, "", validationID, true)
	assert.True(t, validator.IsValid())
	assert.Equal(t, 0, len(validator.Errors))
}

func TestBag_SampleGood(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("example.edu.sample_good.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, "", validationID, true)
	assert.True(t, validator.IsValid())
	assert.Equal(t, 0, len(validator.Errors))
}

func TestBag_SampleMissingDataFile(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("example.edu.sample_missing_data_file.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, "", validationID, true)
	assert.False(t, validator.IsValid())
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "File example.edu/example.edu.sample_missing_data_file/data/datastream-DC in manifest-md5.txt is missing from bag", validator.Errors[0])
}

func TestBag_NoAPTrustInfo(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("example.edu.sample_no_aptrust_info.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, "", validationID, true)
	assert.False(t, validator.IsValid())
	assert.Equal(t, 2, len(validator.Errors))

	expected := []string{
		"Required tag Title in file aptrust-info.txt is missing",
		"Required tag Access in file aptrust-info.txt is missing",
	}
	for _, msg := range expected {
		assert.True(t, util.StringListContains(validator.Errors, msg))
	}
}

func TestBag_SampleNoBagInfo(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("example.edu.sample_no_bag_info.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, "", validationID, true)
	assert.False(t, validator.IsValid())
	assert.Equal(t, 2, len(validator.Errors))
	assert.Equal(t,
		"Required tag Source-Organization in file bag-info.txt is missing",
		validator.Errors[0])
	assert.Equal(t,
		"Required tag Access in file aptrust-info.txt is missing",
		validator.Errors[1])
}

func TestBag_SampleNoBagIt(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("example.edu.sample_no_bagit.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, "", validationID, true)
	assert.False(t, validator.IsValid())
	assert.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "Missing required tag bagit.txt/BagIt-Version.", validator.Errors[0])
}

func TestBag_SampleNoDataDir(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("example.edu.sample_no_data_dir.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, "", validationID, true)
	assert.False(t, validator.IsValid())
	assert.Equal(t, 4, len(validator.Errors))

	expected := []string{
		"File example.edu/example.edu.sample_no_data_dir/data/datastream-DC in manifest-md5.txt is missing from bag",
		"File example.edu/example.edu.sample_no_data_dir/data/datastream-RELS-EXT in manifest-md5.txt is missing from bag",
		"File example.edu/example.edu.sample_no_data_dir/data/datastream-descMetadata in manifest-md5.txt is missing from bag",
		"File example.edu/example.edu.sample_no_data_dir/data/datastream-MARC in manifest-md5.txt is missing from bag",
	}
	for _, msg := range expected {
		assert.True(t, util.StringListContains(validator.Errors, msg),
			"Missing expected error: "+msg)
	}
}

func TestBag_SampleNoMd5Manifest(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("example.edu.sample_no_md5_manifest.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, "", validationID, true)
	assert.False(t, validator.IsValid())
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "Bag is missing required manifest 'md5'", validator.Errors[0])
}

func TestBag_SampleNoTitle(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("example.edu.sample_no_title.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, "", validationID, true)
	assert.False(t, validator.IsValid())
	assert.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "In file aptrust-info.txt, required tag Title has no value", validator.Errors[0])
}

func TestBag_TagSampleBad(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("example.edu.tagsample_bad.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, "", validationID, true)
	assert.False(t, validator.IsValid())
	assert.Equal(t, 3, len(validator.Errors))
	assert.Equal(t,
		"In file aptrust-info.txt, required tag Title has no value",
		validator.Errors[0])
	assert.Equal(t,
		"In file aptrust-info.txt, tag Access has illegal value 'acksess'",
		validator.Errors[1])
	assert.Equal(t,
		"In file aptrust-info.txt, tag Storage-Option has illegal value 'Cardboard-Box'",
		validator.Errors[2])
}

func TestBag_TagSampleGood(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("example.edu.tagsample_good.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, goodbagMd5, validationID, true)
	assert.True(t, validator.IsValid())
	assert.Equal(t, 0, len(validator.Errors))
}

func TestBag_SampleIllegalControl(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("test.edu.sample_illegal_control.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileDefault, pathToBag, "", validationID, true)
	assert.False(t, validator.IsValid())
	assert.Equal(t, 1, len(validator.Errors))

	expected := []string{
		"File name 'data/datastream\\u007f.txt' contains one or more illegal control characters",
	}

	for _, msg := range expected {
		assert.True(t, util.StringListContains(validator.Errors, msg))
	}
}

// ------ BTR Bags ------

func TestBag_BTR_Good256(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("test.edu.btr_good_sha256.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileBTR, pathToBag, "", validationID, true)
	assert.True(t, validator.IsValid())
	assert.Equal(t, 0, len(validator.Errors))
}

func TestBag_BTR_Good512(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("test.edu.btr_good_sha512.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileBTR, pathToBag, "", validationID, true)
	assert.True(t, validator.IsValid())
	assert.Equal(t, 0, len(validator.Errors))
}

func TestBag_BTR_BadChecksums(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("test.edu.btr_bad_checksums.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileBTR, pathToBag, "", validationID, true)
	assert.False(t, validator.IsValid())
	expected := []string{
		"File test.edu/test.edu.btr_bad_checksums/manifest-sha512.txt: ingest sha512 checksum fc9a6f7dfac6ab9edf60858391d90131671fc18f1ca6c15fe7bd652a5a0059e76c023cfdf325305d659f2d9719a12c3c97b6d9ead8d4bff3177ab5afdfdf7483 doesn't match manifest checksum aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"File test.edu/test.edu.btr_bad_checksums/data/netutil/listen.go: ingest sha512 checksum b598afc318e6d06f54a162e8e43bbc9cb071fcf0ffb3766b719011d8403d01290d6f2d7a9decc504395501f28f6c452c5a4317ee7bd309d4cd597984227d176d doesn't match manifest checksum 00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
	}
	require.Equal(t, 2, len(validator.Errors))
	for _, msg := range expected {
		assert.True(t, util.StringListContains(validator.Errors, msg), msg)
	}
}

func TestBag_BTR_BadExtraFile(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("test.edu.btr_bad_extraneous_file.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileBTR, pathToBag, "", validationID, true)
	assert.False(t, validator.IsValid())
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "File test.edu/test.edu.btr_bad_extraneous_file/data/nsqd.dat is not in manifest manifest-sha512.txt", validator.Errors[0])
}

func TestBag_BTR_MissingPayload(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("test.edu.btr_bad_missing_payload_file.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileBTR, pathToBag, "", validationID, true)
	assert.False(t, validator.IsValid())
	require.Equal(t, 1, len(validator.Errors))
	assert.Equal(t, "File test.edu/test.edu.btr_bad_missing_payload_file/data/netutil/listen.go in manifest-sha512.txt is missing from bag", validator.Errors[0])
}

func TestBag_BTR_MissingTags(t *testing.T) {
	pathToBag := testutil.PathToUnitTestBag("test.edu.btr_bad_missing_required_tags.tar")
	validator := setupValidatorAndObject(t,
		constants.BagItProfileBTR, pathToBag, "", validationID, true)
	assert.False(t, validator.IsValid())
	expected := []string{
		"Required tag Bagging-Date in file bag-info.txt is missing",
		"Required tag Payload-Oxum in file bag-info.txt is missing",
		"Required tag Source-Organization in file bag-info.txt is missing",
	}
	require.Equal(t, 3, len(validator.Errors))
	for _, msg := range expected {
		assert.True(t, util.StringListContains(validator.Errors, msg), msg)
	}
}
