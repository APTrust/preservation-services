package common_test

import (
	"fmt"
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
	assert.Equal(t, "password", config.RegistryAPIKey)
	assert.Equal(t, "system@aptrust.org", config.RegistryAPIUser)
	assert.Equal(t, "v3", config.RegistryAPIVersion)
	assert.Equal(t, "http://localhost:8080", config.RegistryURL)
	assert.Equal(t, 0, config.RedisDefaultDB)
	assert.Equal(t, "", config.RedisPassword)
	assert.Equal(t, 3, config.RedisRetries)
	assert.Equal(t, time.Duration(250*time.Millisecond), config.RedisRetryMs)
	assert.Equal(t, "localhost:6379", config.RedisURL)
	assert.Equal(t, "", config.RedisUser)
	assert.Equal(t, "password", config.RegistryAPIKey)
	assert.Equal(t, "system@aptrust.org", config.RegistryAPIUser)
	assert.Equal(t, "v3", config.RegistryAPIVersion)
	assert.Equal(t, "http://localhost:8080", config.RegistryURL)
	assert.Equal(t, restoreDir, config.RestoreDir)
	assert.Equal(t, "staging", config.StagingBucket)
	assert.Equal(t, time.Duration(250*time.Millisecond), config.StagingUploadRetryMs)
	assert.Equal(t, "http://localhost:8898", config.VolumeServiceURL)

	require.Equal(t, 5, len(config.S3Credentials))

	// In test env, these are all set to the local minio instance,
	// so we don't save/delete/overwrite in any external services.
	for _, name := range constants.StorageProviders {
		provider := config.S3Credentials[name]
		assert.Equal(t, "localhost:9899", provider.Host)
		assert.Equal(t, "minioadmin", provider.KeyID)
		assert.Equal(t, "minioadmin", provider.SecretKey)
	}

	assert.Equal(t, 42, len(config.WorkerSettings))
	for _, value := range config.WorkerSettings {
		assert.True(t, value > 0)
		assert.True(t, value < 100)
	}
}

func TestPreservationBucketsFor(t *testing.T) {
	config := common.NewConfig()
	preservationBuckets := config.PreservationBucketsFor(constants.StorageStandard)
	require.Equal(t, 2, len(preservationBuckets))
	for _, preservationBucket := range preservationBuckets {
		assert.Equal(t, constants.StorageStandard, preservationBucket.OptionName)
	}

	preservationBuckets = config.PreservationBucketsFor(constants.StorageWasabiVA)
	require.Equal(t, 1, len(preservationBuckets))
	assert.Equal(t, constants.StorageWasabiVA, preservationBuckets[0].OptionName)
}

func TestPreservationBucketForUrl(t *testing.T) {
	config := common.NewConfig()
	for _, bucket := range config.PreservationBuckets {
		_url := fmt.Sprintf("https://%s/%s/somefile.txt", bucket.Host, bucket.Bucket)
		b := config.PreservationBucketForUrl(_url)
		require.NotNil(t, b)
		assert.Equal(t, bucket.Host, b.Host)
		assert.Equal(t, bucket.Region, b.Region)
		assert.Equal(t, bucket.Bucket, b.Bucket)
	}
	b := config.PreservationBucketForUrl("https://no.such.host/no-such-bucket/file.txt")
	assert.Nil(t, b)
}

func TestGetWorkerSettings(t *testing.T) {
	config := common.NewConfig()

	// When params are > 0, should return params as-is.
	// Params > 0 were passed in on the command line.
	bufSize, numWorkers, maxAttempts := config.GetWorkerSettings(
		constants.IngestReingestCheck,
		30,
		20,
		10)
	assert.Equal(t, 30, bufSize)
	assert.Equal(t, 20, numWorkers)
	assert.Equal(t, 10, maxAttempts)

	// Params <= 0 means the arg was not specified on the command line.
	// When params are <= 0, should return values from .env file.
	// In testing, these values come from .env.test
	bufSize, numWorkers, maxAttempts = config.GetWorkerSettings(
		constants.IngestReingestCheck,
		-1,
		-1,
		-1)
	assert.Equal(t, 20, bufSize)    // REINGEST_MANAGER_BUFFER_SIZE
	assert.Equal(t, 3, numWorkers)  // REINGEST_MANAGER_WORKERS
	assert.Equal(t, 3, maxAttempts) // REINGEST_MANAGER_MAX_ATTEMPTS
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
		"RegistryAPIVersion",
		"RegistryURL",
		"RedisDefaultDB",
		"RegistryAPIVersion",
		"RegistryURL",
		"WorkerSettings",
	}
	for _, key := range expectedKeys {
		assert.True(t, strings.Contains(jsonString, key))
	}

	sensitiveKeys := []string{
		"RegistryAPIKey",
		"RegistryAPIUser",
		"RedisPassword",
		"RegistryAPIKey",
		"RegistryAPIUser",
		"RedisUser",
		"S3Credentials",
	}
	for _, key := range sensitiveKeys {
		assert.False(t, strings.Contains(jsonString, key), key)
	}
}

func TestBucketAndKeyFor(t *testing.T) {
	config := common.NewConfig()

	bucket, key, err := config.BucketAndKeyFor("https://s3.us-west-1.localhost:9899/wasabi-or/1234")
	require.Nil(t, err)
	require.NotNil(t, bucket)
	assert.Equal(t, constants.StorageProviderWasabiOR, bucket.Provider)
	assert.Equal(t, config.BucketWasabiOR, bucket.Bucket)
	assert.Equal(t, "1234", key)

	bucket, key, err = config.BucketAndKeyFor("https://s3.us-east-1.localhost:9899/glacier-deep-va/nested/key/5678")
	require.Nil(t, err)
	require.NotNil(t, bucket)
	assert.Equal(t, constants.StorageProviderAWS, bucket.Provider)
	assert.Equal(t, config.BucketGlacierDeepVA, bucket.Bucket)
	assert.Equal(t, "nested/key/5678", key)

	// With region prefix
	bucket, key, err = config.BucketAndKeyFor("https://s3.us-east-1.localhost:9899/preservation-va/nested/key/5678")
	require.Nil(t, err)
	require.NotNil(t, bucket)
	assert.Equal(t, constants.StorageProviderAWS, bucket.Provider)
	assert.Equal(t, config.BucketStandardVA, bucket.Bucket)
	assert.Equal(t, "nested/key/5678", key)

	// Not bucket or key
	bucket, key, err = config.BucketAndKeyFor("https://localhost:9899")
	assert.NotNil(t, err)

	// Unknown provider
	bucket, key, err = config.BucketAndKeyFor("https://example.com/wont-work")
	assert.NotNil(t, err)
}
