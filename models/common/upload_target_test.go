package common_test

import (
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/stretchr/testify/assert"
)

func TestUploadTargetURLFor(t *testing.T) {
	target := &common.UploadTarget{
		Bucket:       "test-bucket",
		Description:  "Test target",
		OptionName:   "FakeStorageOption",
		Provider:     constants.StorageProviderAWS,
		Region:       constants.RegionAWSUSEast2,
		StorageClass: constants.StorageClassStandard,
	}
	expected := "https://s3.us-east-2.amazonaws.com/test-bucket/abc"
	assert.Equal(t, expected, target.URLFor("abc"))

	target.Provider = constants.StorageProviderWasabi
	target.Region = constants.RegionWasabiUSWest1

	expected = "https://s3.us-west-1.wasabisys.com/test-bucket/xyz"
	assert.Equal(t, expected, target.URLFor("xyz"))
}
