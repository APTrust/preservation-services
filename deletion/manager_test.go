// +build integration

package deletion_test

import (
	"fmt"
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/deletion"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These objects are loaded as part of the fixture data.
// Note the the generic file records exist in Pharos only.
// There are no corresponding files in the Minio preservation
// buckets until we put them there.
var objId = "institution2.edu/coal"
var fileNames = []string{
	"doc1",
	"doc2",
	"doc3",
}
var alreadySaved = make([]string, 0)

func TestNewManager(t *testing.T) {
	context := common.NewContext()
	manager := deletion.NewManager(
		context,
		9999,
		"test.edu/my_object",
		constants.TypeObject,
		"requestor@example.com",
		"approver@example.com",
		"some-admin@aptrust.org",
	)
	assert.NotNil(t, manager)
	assert.Equal(t, context, manager.Context)
	assert.Equal(t, 9999, manager.WorkItemID)
	assert.Equal(t, "test.edu/my_object", manager.Identifier)
	assert.Equal(t, constants.TypeObject, manager.ItemType)
	assert.Equal(t, "requestor@example.com", manager.RequestedBy)
	assert.Equal(t, "approver@example.com", manager.InstApprover)
	assert.Equal(t, "some-admin@aptrust.org", manager.APTrustApprover)
}

func TestRun_SingleFile(t *testing.T) {
	context := common.NewContext()
	prepareForTest(t, context)
	fileIdentifier := fmt.Sprintf("institution2.edu/coal/doc1")
	manager := deletion.NewManager(
		context,
		9999,
		fileIdentifier,
		constants.TypeFile,
		"requestor@example.com",
		"approver@example.com",
		"some-admin@aptrust.org",
	)
	count, errors := manager.Run()
	assert.Equal(t, 1, count)
	assert.Empty(t, errors)

	// TODO: make sure file state is 'D'
	// make sure all storage records were removed
	// make sure all deletion events were created with correct info
}

// func TestRun_Object(t *testing.T) {
// 	context := common.NewContext()
// 	prepareForTest(t, context)
// 	fileIdentifier := fmt.Sprintf("institution2.edu/coal")
// 	manager := deletion.NewManager(
// 		context,
// 		9999,
// 		fileIdentifier,
// 		constants.TypeObject,
// 		"requestor@example.com",
// 		"approver@example.com",
// 		"some-admin@aptrust.org",
// 	)
// 	count, errors := manager.Run()
// 	assert.Equal(t, 0, count)
// 	assert.Empty(t, errors)

// 	// TODO: Create deletion workitem for this object,
// 	// or Pharos returns this error when you call the object's
// 	// finish_delete endpoint:
// 	// "There is no existing deletion request for the specified object."

// 	// TODO: make sure object state is 'D'
// 	// make sure all file states are 'D'
// 	// make sure all storage records were removed
// 	// make sure all deletion events were created with correct info
// 	// at both the object and file level
// }

func prepareForTest(t *testing.T, context *common.Context) {
	markObjectActive(t, context)
	copyFilesToLocalPreservation(t, context)
}

func markObjectActive(t *testing.T, context *common.Context) {
	resp := context.PharosClient.IntellectualObjectGet(objId)
	require.Nil(t, resp.Error)
	obj := resp.IntellectualObject()
	require.NotNil(t, obj)
	obj.State = constants.StateActive
	resp = context.PharosClient.IntellectualObjectSave(obj)
	require.Nil(t, resp.Error)
}

func copyFilesToLocalPreservation(t *testing.T, context *common.Context) {
	for _, filename := range fileNames {
		copyFileToBuckets(t, context, filename)
	}
}

// This copies a file into each of the preservation buckets. Note that we
// copy the same file every time. We just give it a different key name in
// the preservation bucket. For the puposes of our test, all we care about
// is whether the files are deleted by the end.
func copyFileToBuckets(t *testing.T, context *common.Context, filename string) {
	pathToFile := testutil.PathToUnitTestBag("example.edu.multipart.b01.of02.tar")
	gfIdentifier := fmt.Sprintf("%s/%s", objId, filename)
	for _, target := range context.Config.UploadTargets {
		_url := target.URLFor(filename)
		if !util.StringListContains(alreadySaved, _url) {
			continue
		}
		client := context.S3Clients[target.Provider]
		_, err := client.FPutObject(
			target.Bucket,
			filename,
			pathToFile,
			minio.PutObjectOptions{},
		)
		require.Nil(t, err)

		storageRecord := &registry.StorageRecord{
			URL: _url,
		}
		resp := context.PharosClient.StorageRecordSave(storageRecord, gfIdentifier)
		require.Nil(t, resp.Error)
		alreadySaved = append(alreadySaved, _url)
	}
}
