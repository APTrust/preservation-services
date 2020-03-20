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

func TestCanRun(t *testing.T) {
	f := util.NewFormatIdentifier()
	assert.True(t, f.CanRun())
}

func TestPathToFido(t *testing.T) {
	f := util.NewFormatIdentifier()
	assert.True(t, strings.HasSuffix(f.PathToFido(), "fido"))
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
}

func TestParseOutput(t *testing.T) {
	f := util.NewFormatIdentifier()
	idRecord := f.ParseOutput("OK,text/html,signature")
	assert.Equal(t, "text/html", idRecord.MimeType)
	assert.Equal(t, "signature", idRecord.MatchType)
	assert.True(t, idRecord.Succeeded)

	idRecord = f.ParseOutput("FAIL,,fail")
	assert.Equal(t, "", idRecord.MimeType)
	assert.Equal(t, "fail", idRecord.MatchType)
	assert.False(t, idRecord.Succeeded)
}

func TestIdentify(t *testing.T) {
	f := util.NewFormatIdentifier()
	idRecord, err := f.Identify("https://google.com", "index.html")
	assert.Nil(t, err)
	assert.NotNil(t, idRecord)
	assert.True(t, idRecord.Succeeded)
	assert.Equal(t, "text/html", idRecord.MimeType)
	assert.Equal(t, "signature", idRecord.MatchType)

	idRecord, err = f.Identify("https://example.com/doesnotexist", "index.html")
	assert.Nil(t, err)
	assert.NotNil(t, err)
	assert.Nil(t, idRecord)
}
