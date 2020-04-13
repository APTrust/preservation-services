// +build integration

package ingest_test

import (
	"path"
	"testing"

	//"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	//"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const recorderItemID_01 = 32998

func getBagPath(folder, filename string) string {
	return path.Join(testutil.PathToTestData(), "int_test_bags", folder, filename)
}

func TestNewRecorder(t *testing.T) {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	recorder := ingest.NewRecorder(context, 333, obj)
	require.NotNil(t, recorder)
	assert.Equal(t, context, recorder.Context)
	assert.Equal(t, obj, recorder.IngestObject)
	assert.Equal(t, 333, recorder.WorkItemID)
}

func TestRecordAll(t *testing.T) {
	context := common.NewContext()
	bagPath := getBagPath("original", "test.edu.apt-001.tar")
	recorder := prepareForRecord(t, bagPath, recorderItemID_01, context)
	require.NotNil(t, recorder)
	fileCount, errors := recorder.RecordAll()
	require.Empty(t, errors, errors)
	require.True(t, fileCount > 0)
}
