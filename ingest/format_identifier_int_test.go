// +build integration

package ingest_test

import (
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	assert.NotNil(t, f.Identifier)
	return f
}

func TestNewFormatIdentifier(t *testing.T) {
	getFormatIdentifier(t)
}
