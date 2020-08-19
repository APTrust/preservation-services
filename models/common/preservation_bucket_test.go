package common_test

import (
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/stretchr/testify/assert"
)

func getTarget() *common.PerservationBucket {
	return &common.PerservationBucket{
		Bucket:       "test-bucket",
		Description:  "Test target",
		Host:         "flava.flave",
		OptionName:   "FakeStorageOption",
		Provider:     constants.StorageProviderAWS,
		Region:       constants.RegionAWSUSEast2,
		StorageClass: constants.StorageClassStandard,
	}
}

func TestPerservationBucketURLFor(t *testing.T) {
	target := getTarget()
	expected := "https://s3.us-east-2.flava.flave/test-bucket/abc"
	assert.Equal(t, expected, target.URLFor("abc"))

	target.Provider = constants.StorageProviderWasabi
	target.Region = constants.RegionWasabiUSWest1

	expected = "https://s3.us-west-1.flava.flave/test-bucket/xyz"
	assert.Equal(t, expected, target.URLFor("xyz"))
}

func TestHostsURL(t *testing.T) {
	target := getTarget()
	url1 := "https://s3.us-east-2.flava.flave/test-bucket/abc"
	url2 := "https://s3.us-west-1.flava.flave/test-bucket/xyz"
	assert.True(t, target.HostsURL(url1))
	assert.False(t, target.HostsURL(url2))

	target.Provider = constants.StorageProviderWasabi
	target.Region = constants.RegionWasabiUSWest1
	assert.False(t, target.HostsURL(url1))
	assert.True(t, target.HostsURL(url2))
}
