package common_test

import (
	"github.com/APTrust/preservation-services/models/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewConfig(t *testing.T) {
	config := common.NewConfig()
	assert.Equal(t, "test", config.ConfigName)
}

func TestDefaultS3ClientName(t *testing.T) {
	config := common.NewConfig()
	assert.Equal(t, "test", config.ConfigName)
	assert.Equal(t, "LocalTest", config.DefaultS3ClientName())

	config.ConfigName = "staging"
	assert.Equal(t, "AWS", config.DefaultS3ClientName())
}
