package util_test

import (
	"fmt"
	"github.com/APTrust/preservation-services/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestSystemHasIdentifierPrograms(t *testing.T) {
	ok, missing := util.SystemHasIdentifierPrograms()
	assert.True(t, ok)
	assert.Equal(t, 0, len(missing),
		fmt.Sprintf("Missing: %s", strings.Join(missing, ", ")))
}

func TestValidateParams(t *testing.T) {
	f := util.NewFormatIdentifier()
	assert.NotNil(t, f.ValidateParams("https://example.com", ""))
	assert.NotNil(t, f.ValidateParams("", "filename"))
	assert.NotNil(t, f.ValidateParams("https://unsafe.chars/{$}", "filename"))
	assert.NotNil(t, f.ValidateParams("https://example.com", "un`'`chars"))
	assert.Nil(t, f.ValidateParams("https://example.com", "index.html"))
}

func TestGetCommandString(t *testing.T) {
	f := util.NewFormatIdentifier()

	// Will be nil with error for invalid params
	cmdString, err := f.GetCommandString("", "")
	assert.NotNil(t, err)
	assert.Empty(t, cmdString)

	cmdString, err = f.GetCommandString("https://example.com", "index.html")
	require.Nil(t, err)
	assert.True(t, strings.Contains(cmdString, "curl"))
	assert.True(t, strings.Contains(cmdString, "python2"))
	assert.True(t, strings.Contains(cmdString, "fido"))
	assert.True(t, strings.Contains(cmdString, "https://example.com"))
	assert.True(t, strings.Contains(cmdString, "index.html"))
	assert.True(t, strings.Contains(cmdString, "zzzzzzzzzz"), cmdString)
}
