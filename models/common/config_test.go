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

// TODO: Test that different configs get the right settings.
