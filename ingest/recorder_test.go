// +build integration

package ingest_test

import (
	"testing"

	//"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	//"github.com/APTrust/preservation-services/models/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRecorder(t *testing.T) {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	recorder := ingest.NewRecorder(context, testWorkItemId, obj)
	require.NotNil(t, recorder)
	assert.Equal(t, context, recorder.Context)
	assert.Equal(t, obj, recorder.IngestObject)
	assert.Equal(t, testWorkItemId, recorder.WorkItemID)
}
