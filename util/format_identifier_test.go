package util_test

import (
	"fmt"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/util"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func getScriptPath() string {
	context := common.NewContext()
	return context.Config.FormatIdentifierScript()
}

func TestSystemHasIdentifierPrograms(t *testing.T) {
	ok, missing := util.SystemHasIdentifierPrograms(getScriptPath())
	assert.True(t, ok)
	assert.Equal(t, 0, len(missing),
		fmt.Sprintf("Missing: %s", strings.Join(missing, ", ")))
}

func TestCanRun(t *testing.T) {
	f := util.NewFormatIdentifier(getScriptPath())
	assert.True(t, f.CanRun())
}

func TestValidateParams(t *testing.T) {
	f := util.NewFormatIdentifier(getScriptPath())
	assert.NotNil(t, f.ValidateParams("https://example.com", ""))
	assert.NotNil(t, f.ValidateParams("", "filename"))
	assert.NotNil(t, f.ValidateParams("https://unsafe.chars/{$}", "filename"))
	assert.NotNil(t, f.ValidateParams("https://example.com", "un`'`chars"))
	assert.Nil(t, f.ValidateParams("https://example.com", "index.html"))
}

func TestParseOutput(t *testing.T) {
	f := util.NewFormatIdentifier(getScriptPath())
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
	f := util.NewFormatIdentifier(getScriptPath())
	idRecord, err := f.Identify("https://www.google.com", "index.html")
	assert.Nil(t, err)
	assert.NotNil(t, idRecord)
	assert.True(t, idRecord.Succeeded)
	assert.Equal(t, "text/html", idRecord.MimeType)
	assert.Equal(t, "signature", idRecord.MatchType)

	// 404 error
	idRecord, err = f.Identify("https://example.com/doesnotexist", "index.html")
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "Server returned status code 404"))
	assert.Nil(t, idRecord)

	// Connection refused
	idRecord, err = f.Identify("https://localhost:0", "index.html")
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "No response or connection refused"))
	assert.Nil(t, idRecord)
}
