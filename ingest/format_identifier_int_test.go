// +build integration

package ingest_test

import (
	//"github.com/APTrust/preservation-services/bagit"
	//"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	//"github.com/APTrust/preservation-services/util/testutil"
	//"github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	//"path"
	"testing"
)

func getFormatIdentifier(t *testing.T) *ingest.FormatIdentifier {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	f := ingest.NewFormatIdentifier(context, suWorkItemId, obj)
	require.NotNil(t, f)
	assert.Equal(t, context, f.Context)
	assert.Equal(t, obj, f.IngestObject)
	assert.Equal(t, suWorkItemId, f.WorkItemId)
	assert.NotEqual(t, "", f.CurlCmd)
	assert.NotEqual(t, "", f.PythonCmd)
	return f
}

func TestNewFormatIdentifier(t *testing.T) {
	getFormatIdentifier(t)
}

func TestPathTo(t *testing.T) {
	f := getFormatIdentifier(t)
	assert.NotPanics(t, func() {
		f.PathTo("ls")
	}, "PathTo with legit program should not panic")

	path := f.PathTo("ls")
	assert.NotEqual(t, "", path)

	assert.Panics(t, func() {
		f.PathTo("xxxxyyyyzzzz")
	}, "PathTo with non-existent program should panic")
}

func TestGetBaseCommands(t *testing.T) {

}

func TestGetCommandString(t *testing.T) {

}
