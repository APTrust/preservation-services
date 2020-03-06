package constants_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestStorageURIs(t *testing.T) {
	// Make sure these end with a slash!
	for _, opt := range constants.StorageOptions {
		uri := constants.BaseURIFor[opt]
		require.NotNil(t, uri)
		assert.True(t, strings.HasSuffix(uri, "/"))
	}
}
