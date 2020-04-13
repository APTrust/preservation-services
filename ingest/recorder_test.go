// +build integration

package ingest_test

import (
	//"fmt"
	"net/url"
	"path"
	"testing"

	"github.com/APTrust/preservation-services/constants"
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
	require.Empty(t, errors)
	assert.Equal(t, 18, fileCount)

	testNewObjectInPharos(t, recorder)
	testNewFilesInPharos(t, recorder)
}

func testNewObjectInPharos(t *testing.T, recorder *ingest.Recorder) {
	client := recorder.Context.PharosClient
	resp := client.IntellectualObjectGet(recorder.IngestObject.Identifier())
	require.Nil(t, resp.Error)
	intelObj := resp.IntellectualObject()
	require.NotNil(t, intelObj)

	assert.Equal(t, intelObj.Access, constants.AccessInstitution)
	assert.Equal(t, intelObj.AltIdentifier, "bag001")
	assert.Equal(t, intelObj.BagGroupIdentifier, "apt-001")
	assert.Equal(t, intelObj.BagItProfileIdentifier, "https://raw.githubusercontent.com/APTrust/preservation-services/master/profiles/aptrust-v2.2.json")
	assert.Equal(t, intelObj.BagName, "test.edu.apt-001")
	assert.False(t, intelObj.CreatedAt.IsZero())
	assert.Equal(t, intelObj.Description, "Test bag 001 for integration tests")
	assert.Equal(t, 32, len(intelObj.ETag))
	assert.True(t, intelObj.ID > 0)
	assert.Equal(t, intelObj.Identifier, "test.edu/test.edu.apt-001")
	assert.Equal(t, intelObj.Institution, "test.edu")

	// Also, Internal-Sender-Identifier and Internal-Sender-Description
	// are not being saved.

	assert.True(t, intelObj.InstitutionID > 0)
	assert.Equal(t, intelObj.SourceOrganization, "Test University")
	assert.Equal(t, intelObj.State, "A")
	assert.Equal(t, intelObj.StorageOption, constants.StorageClassStandard)
	assert.Equal(t, intelObj.Title, "APTrust Test Bag 001")
	assert.False(t, intelObj.UpdatedAt.IsZero())

}

func testNewFilesInPharos(t *testing.T, recorder *ingest.Recorder) {
	client := recorder.Context.PharosClient
	params := url.Values{}
	params.Add("intellectual_object_identifier", recorder.IngestObject.Identifier())
	params.Add("per_page", "100")
	params.Add("page", "1")
	resp := client.GenericFileList(params)
	//data, _ := resp.RawResponseData()
	//fmt.Println(string(data))
	require.Nil(t, resp.Error)
	genericFiles := resp.GenericFiles()
	require.NotEmpty(t, genericFiles)
	// for _, gf := range genericFiles {
	// 	data, _ := gf.ToJSON()
	// 	assert.Equal(t, "x", string(data))
	// }
}
