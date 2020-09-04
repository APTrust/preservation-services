package common_test

import (
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/stretchr/testify/assert"
)

func getBucket() *common.PerservationBucket {
	return &common.PerservationBucket{
		Bucket:       "test-bucket",
		Description:  "Test bucket",
		Host:         "s3.flava.flave",
		OptionName:   "FakeStorageOption",
		Provider:     constants.StorageProviderAWS,
		Region:       constants.RegionAWSUSEast2,
		StorageClass: constants.StorageClassStandard,
	}
}

func TestPerservationBucketURLFor(t *testing.T) {
	preservationBucket := getBucket()
	expected := "https://s3.us-east-2.flava.flave/test-bucket/abc"
	assert.Equal(t, expected, preservationBucket.URLFor("abc"))

	preservationBucket.Provider = constants.StorageProviderWasabi
	preservationBucket.Region = constants.RegionWasabiUSWest1

	expected = "https://s3.us-west-1.flava.flave/test-bucket/xyz"
	assert.Equal(t, expected, preservationBucket.URLFor("xyz"))
}

func TestHostsURL(t *testing.T) {
	preservationBucket := getBucket()
	url1 := "https://s3.us-east-2.flava.flave/test-bucket/abc"
	url2 := "https://s3.us-west-1.flava.flave/test-bucket/xyz"
	assert.True(t, preservationBucket.HostsURL(url1))
	assert.False(t, preservationBucket.HostsURL(url2))

	preservationBucket.Provider = constants.StorageProviderWasabi
	preservationBucket.Region = constants.RegionWasabiUSWest1
	assert.False(t, preservationBucket.HostsURL(url1))
	assert.True(t, preservationBucket.HostsURL(url2))
}
