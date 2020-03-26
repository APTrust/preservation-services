package common_test

import (
	"path"
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
	assert.True(t, strings.HasSuffix(config.ScriptDir, "scripts"))
	assert.Equal(t, "staging", config.StagingBucket)
	assert.Equal(t, 3, config.StagingUploadRetries)
	assert.Equal(t, time.Duration(250*time.Millisecond), config.StagingUploadRetryMs)
	assert.Equal(t, "http://localhost:8898", config.VolumeServiceURL)

	require.Equal(t, 3, len(config.S3Credentials))

	// In test env, these are all set to the local minio instance,
	// so we don't save/delete/overwrite in any external services.
	for _, name := range constants.StorageProviders {
		provider := config.S3Credentials[name]
		assert.Equal(t, "localhost:9899", provider.Host)
		assert.Equal(t, "minioadmin", provider.KeyID)
		assert.Equal(t, "minioadmin", provider.SecretKey)
	}
}

func TestPathToScript(t *testing.T) {
	config := common.NewConfig()
	script := config.PathToScript("identify_format.sh")
	assert.True(t, strings.HasSuffix(script, path.Join("scripts", "identify_format.sh")))
}

func TestUploadTargetsFor(t *testing.T) {
	config := common.NewConfig()
	targets := config.UploadTargetsFor(constants.StorageStandard)
	require.Equal(t, 2, len(targets))
	for _, target := range targets {
		assert.Equal(t, constants.StorageStandard, target.OptionName)
	}

	targets = config.UploadTargetsFor(constants.StorageWasabiVA)
	require.Equal(t, 1, len(targets))
	assert.Equal(t, constants.StorageWasabiVA, targets[0].OptionName)
}

// TODO: Test that different configs get the right settings.
