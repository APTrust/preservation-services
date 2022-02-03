//go:build integration
// +build integration

package network_test

import (
	//	"bytes"
	//	"encoding/json"
	//	"fmt"
	//	"net/url"
	//	"strings"
	"testing"

	//	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	//	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/network"
	"github.com/APTrust/preservation-services/util/logger"
	//	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Registry rules say we can't restore an item that's being deleted
// or delete an item that's being restored. To avoid errors in our
// integration tests, make sure we test different object for restore
// and delete. These ids come from the integration test fixtures.
// const ObjIdToDelete = "institution2.edu/coal"
// const ObjIdToRestore = "institution2.edu/toads"
// const FileIdToRestore = "institution2.edu/coal/doc3"
// const FileIdWithChecksums = "institution1.edu/photos/picture1"

func GetRegistryClient(t *testing.T) *network.RegistryClient {
	config := common.NewConfig()
	assert.Equal(t, "test", config.ConfigName)
	_logger, _ := logger.InitLogger(config.LogDir, config.LogLevel)
	require.NotNil(t, _logger)
	client, err := network.NewRegistryClient(
		config.RegistryURL,
		config.RegistryAPIVersion,
		config.RegistryAPIUser,
		config.RegistryAPIKey,
		_logger,
	)
	require.Nil(t, err)
	require.NotNil(t, client)
	return client
}
