package common_test

import (
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/stretchr/testify/assert"
)

func getBucket() *common.PreservationBucket {
	return &common.PreservationBucket{
		Bucket:       "test-bucket",
		Description:  "Test bucket",
		Host:         "s3.flava.flave",
		OptionName:   "FakeStorageOption",
		Provider:     constants.StorageProviderAWS,
		Region:       constants.RegionAWSUSEast2,
		StorageClass: constants.StorageClassStandard,
	}
}

func TestPreservationBucketURLFor(t *testing.T) {
	preservationBucket := getBucket()
	expected := "https://s3.us-east-2.flava.flave/test-bucket/abc"
	assert.Equal(t, expected, preservationBucket.URLFor("abc"))

	preservationBucket.Provider = constants.StorageProviderWasabiOR
	preservationBucket.Region = constants.RegionWasabiUSWest1

	expected = "https://s3.us-west-1.flava.flave/test-bucket/xyz"
	assert.Equal(t, expected, preservationBucket.URLFor("xyz"))
}

func TestHostsURL(t *testing.T) {
	preservationBucket := getBucket()

	// Should match url with correct region.
	url1 := "https://s3.us-east-2.flava.flave/test-bucket/abc"
	assert.True(t, preservationBucket.HostsURL(url1))

	// Should not match url with wrong region.
	url2 := "https://s3.us-west-1.flava.flave/test-bucket/xyz"
	assert.False(t, preservationBucket.HostsURL(url2))

	// Should match url with no region.
	url3 := "https://s3.flava.flave/test-bucket/xyz"
	assert.True(t, preservationBucket.HostsURL(url3))

	preservationBucket.Provider = constants.StorageProviderWasabiOR
	preservationBucket.Region = constants.RegionWasabiUSWest1

	// Wrong region -> no match
	assert.False(t, preservationBucket.HostsURL(url1))

	// Right region matches
	assert.True(t, preservationBucket.HostsURL(url2))

	// URL without region matches
	assert.True(t, preservationBucket.HostsURL(url3))
}

func TestGetHostNameWithRegion(t *testing.T) {
	b := getBucket()
	assert.Equal(t, "s3.us-east-2.flava.flave", b.GetHostNameWithRegion())

	b.Host = "flava.flave"
	assert.Equal(t, "s3.us-east-2.flava.flave", b.GetHostNameWithRegion())

	b.Host = "s3.amazonaws.com"
	b.Region = constants.RegionAWSUSWest1
	assert.Equal(t, "s3.us-west-1.amazonaws.com", b.GetHostNameWithRegion())

	b.Host = "amazonaws.com"
	assert.Equal(t, "s3.us-west-1.amazonaws.com", b.GetHostNameWithRegion())

	b.Host = "s3.us-west-1.amazonaws.com"
	assert.Equal(t, "s3.us-west-1.amazonaws.com", b.GetHostNameWithRegion())

	b.Host = "wasabisys.com"
	assert.Equal(t, "s3.us-west-1.wasabisys.com", b.GetHostNameWithRegion())

	b.Host = "s3.wasabisys.com"
	assert.Equal(t, "s3.us-west-1.wasabisys.com", b.GetHostNameWithRegion())

	b.Host = "s3.us-west-1.wasabisys.com"
	assert.Equal(t, "s3.us-west-1.wasabisys.com", b.GetHostNameWithRegion())
}

func TestRegionIsEmbedded(t *testing.T) {
	b := getBucket()
	assert.False(t, b.RegionIsEmbeddedInHostName())

	b.Host = "s3.amazonaws.com"
	assert.False(t, b.RegionIsEmbeddedInHostName())

	b.Host = "amazonaws.com"
	assert.False(t, b.RegionIsEmbeddedInHostName())

	b.Host = "s3.us-west-1.amazonaws.com"
	assert.True(t, b.RegionIsEmbeddedInHostName())

	b.Host = "wasabisys.com"
	assert.False(t, b.RegionIsEmbeddedInHostName())

	b.Host = "s3.wasabisys.com"
	assert.False(t, b.RegionIsEmbeddedInHostName())

	b.Host = "s3.us-west-1.wasabisys.com"
	assert.True(t, b.RegionIsEmbeddedInHostName())
}
