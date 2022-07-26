package common_test

import (
	"path/filepath"
	"testing"

	"github.com/APTrust/preservation-services/partner/common"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerifyOutputFormat(t *testing.T) {
	opts := common.Options{}
	opts.OutputFormat = "text"
	opts.VerifyOutputFormat()
	assert.Empty(t, opts.Errors())

	opts.OutputFormat = "json"
	opts.VerifyOutputFormat()
	assert.Empty(t, opts.Errors())

	opts.OutputFormat = "canary"
	opts.VerifyOutputFormat()
	assert.Equal(t, 1, len(opts.Errors()))
}

func TestMergeConfigFileOptions(t *testing.T) {
	filePath := getConfigFilePath()

	conf := getTestConfig(t)
	require.NotNil(t, conf)

	// Now make sure values are merged correctly.
	// These four options, if not explicitly supplied
	// by the user, should be pulled from the config file.
	opts := &common.Options{
		PathToConfigFile: filePath,
		APTrustAPIKey:    "default key",
		APTrustAPIUser:   "default user",
	}

	assert.Equal(t, "default key", opts.APTrustAPIKey)
	assert.Equal(t, "default user", opts.APTrustAPIUser)

	opts.MergeConfigFileOptions()
	assert.Equal(t, "default key", opts.APTrustAPIKey)
	assert.Equal(t, "default user", opts.APTrustAPIUser)
}

func TestErrors(t *testing.T) {
	opts := common.Options{}
	assert.False(t, opts.HasErrors())
	opts.AddError("oops!")
	assert.True(t, opts.HasErrors())
	opts.AddError("and oops again")
	assert.Equal(t, 2, len(opts.Errors()))
	assert.Equal(t, "oops!\nand oops again", opts.AllErrorsAsString())
	opts.ClearErrors()
	assert.Empty(t, opts.Errors())
}

func getTestConfig(t *testing.T) *common.PartnerConfig {
	conf, err := common.LoadPartnerConfig(getConfigFilePath())
	require.Nil(t, err)
	require.NotNil(t, conf)
	return conf
}

func getConfigFilePath() string {
	return filepath.Join(testutil.PathToTestData(), "config", "partner_config_valid.conf")
}
