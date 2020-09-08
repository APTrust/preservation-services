package common_test

import (
	"strings"
	"testing"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/util"
	"github.com/op/go-logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConfig(t *testing.T) {
	workingDir, _ := util.ExpandTilde("~/tmp")
	tempDir, _ := util.ExpandTilde("~/tmp/pres-serv/ingest")
	logDir, _ := util.ExpandTilde("~/tmp/logs")
	restoreDir, _ := util.ExpandTilde("~/tmp/pres-serv/restore")

	config := common.NewConfig()
	assert.Equal(t, workingDir, config.BaseWorkingDir)
	assert.Equal(t, "preservation-or", config.BucketStandardOR)
	assert.Equal(t, "preservation-va", config.BucketStandardVA)
	assert.Equal(t, "glacier-oh", config.BucketGlacierOH)
	assert.Equal(t, "glacier-or", config.BucketGlacierOR)
	assert.Equal(t, "glacier-va", config.BucketGlacierVA)
	assert.Equal(t, "glacier-deep-oh", config.BucketGlacierDeepOH)
	assert.Equal(t, "glacier-deep-or", config.BucketGlacierDeepOR)
	assert.Equal(t, "glacier-deep-va", config.BucketGlacierDeepVA)
	assert.Equal(t, "wasabi-or", config.BucketWasabiOR)
	assert.Equal(t, "wasabi-va", config.BucketWasabiVA)
	assert.Equal(t, "test", config.ConfigName)
	assert.Equal(t, time.Duration(10*time.Second), config.IngestBucketReaderInterval)
	assert.Equal(t, tempDir, config.IngestTempDir)
	assert.Equal(t, logDir, config.LogDir)
	assert.Equal(t, logging.DEBUG, config.LogLevel)
	assert.Equal(t, 90, config.MaxDaysSinceFixityCheck)
	assert.Equal(t, int64(5497558138880), config.MaxFileSize)
	assert.Equal(t, "localhost:4161", config.NsqLookupd)
	assert.Equal(t, "http://localhost:4151", config.NsqURL)
	assert.Equal(t, "c3958c7b09e40af1d065020484dafa9b2a35cea0", config.PharosAPIKey)
	assert.Equal(t, "system@aptrust.org", config.PharosAPIUser)
	assert.Equal(t, "v2", config.PharosAPIVersion)
	assert.Equal(t, "http://localhost:9292", config.PharosURL)
	assert.Equal(t, 0, config.RedisDefaultDB)
	assert.Equal(t, "", config.RedisPassword)
	assert.Equal(t, 3, config.RedisRetries)
	assert.Equal(t, time.Duration(250*time.Millisecond), config.RedisRetryMs)
	assert.Equal(t, "localhost:6379", config.RedisURL)
	assert.Equal(t, "", config.RedisUser)
	assert.Equal(t, restoreDir, config.RestoreDir)
	assert.Equal(t, "staging", config.StagingBucket)
	assert.Equal(t, 3, config.StagingUploadRetries)
	assert.Equal(t, time.Duration(250*time.Millisecond), config.StagingUploadRetryMs)
	assert.Equal(t, "http://localhost:8898", config.VolumeServiceURL)

	require.Equal(t, 4, len(config.S3Credentials))

	// In test env, these are all set to the local minio instance,
	// so we don't save/delete/overwrite in any external services.
	for _, name := range constants.StorageProviders {
		provider := config.S3Credentials[name]
		assert.Equal(t, "localhost:9899", provider.Host)
		assert.Equal(t, "minioadmin", provider.KeyID)
		assert.Equal(t, "minioadmin", provider.SecretKey)
	}
}

func TestPerservationBucketsFor(t *testing.T) {
	config := common.NewConfig()
	preservationBuckets := config.PerservationBucketsFor(constants.StorageStandard)
	require.Equal(t, 2, len(preservationBuckets))
	for _, preservationBucket := range preservationBuckets {
		assert.Equal(t, constants.StorageStandard, preservationBucket.OptionName)
	}

	preservationBuckets = config.PerservationBucketsFor(constants.StorageWasabiVA)
	require.Equal(t, 1, len(preservationBuckets))
	assert.Equal(t, constants.StorageWasabiVA, preservationBuckets[0].OptionName)
}

func TestToJson(t *testing.T) {
	config := common.NewConfig()
	jsonString := config.ToJSON()

	// It's impossible to test for an exact output, since it will
	// differ on every user's machine. Just make sure a few expected
	// keys are present, and the sensitive ones are not.
	expectedKeys := []string{
		"BaseWorkingDir",
		"BucketStandardOR",
		"BucketStandardVA",
		"BucketGlacierOH",
		"BucketGlacierOR",
		"BucketGlacierVA",
		"BucketGlacierDeepOH",
		"BucketGlacierDeepOR",
		"BucketGlacierDeepVA",
		"BucketWasabiOR",
		"BucketWasabiVA",
		"ConfigName",
		"IngestTempDir",
		"PharosAPIVersion",
		"PharosURL",
		"RedisDefaultDB",
	}
	for _, key := range expectedKeys {
		assert.True(t, strings.Contains(jsonString, key))
	}

	sensitiveKeys := []string{
		"PharosAPIKey",
		"PharosAPIUser",
		"RedisPassword",
		"RedisUser",
		"S3Credentials",
	}
	for _, key := range sensitiveKeys {
		assert.False(t, strings.Contains(jsonString, key))
	}
}

func TestProviderBucketAndKeyFor(t *testing.T) {
	config := common.NewConfig()

	provider, bucket, key, err := config.ProviderBucketAndKeyFor("https://localhost:9899/wasabi-or/1234")
	assert.Equal(t, constants.StorageProviderWasabiOR, provider)
	assert.Equal(t, config.BucketWasabiOR, bucket)
	assert.Equal(t, "1234", key)
	assert.Nil(t, err)

	provider, bucket, key, err = config.ProviderBucketAndKeyFor("https://localhost:9899/glacier-deep-va/nested/key/5678")
	assert.Equal(t, constants.StorageProviderAWS, provider)
	assert.Equal(t, config.BucketGlacierDeepVA, bucket)
	assert.Equal(t, "nested/key/5678", key)
	assert.Nil(t, err)

	// With region prefix
	provider, bucket, key, err = config.ProviderBucketAndKeyFor("https://s3.us-east-1.localhost:9899/preservation-va/nested/key/5678")
	assert.Equal(t, constants.StorageProviderAWS, provider)
	assert.Equal(t, config.BucketStandardVA, bucket)
	assert.Equal(t, "nested/key/5678", key)
	assert.Nil(t, err)

	// Not bucket or key
	provider, bucket, key, err = config.ProviderBucketAndKeyFor("https://localhost:9899")
	assert.NotNil(t, err)

	// Unknown provider
	provider, bucket, key, err = config.ProviderBucketAndKeyFor("https://example.com/wont-work")
	assert.NotNil(t, err)
}
