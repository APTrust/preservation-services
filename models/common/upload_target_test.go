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
		Host:         "flava.flave",
		OptionName:   "FakeStorageOption",
		Provider:     constants.StorageProviderAWS,
		Region:       constants.RegionAWSUSEast2,
		StorageClass: constants.StorageClassStandard,
	}
	expected := "https://s3.us-east-2.flava.flave/test-bucket/abc"
	assert.Equal(t, expected, target.URLFor("abc"))

	target.Provider = constants.StorageProviderWasabi
	target.Region = constants.RegionWasabiUSWest1

	expected = "https://s3.us-west-1.flava.flave/test-bucket/xyz"
	assert.Equal(t, expected, target.URLFor("xyz"))
}
