// +build integration

package deletion_test

import (
	"fmt"
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/deletion"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
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

func TestNewManager(t *testing.T) {
	context := common.NewContext()
	manager := deletion.NewManager(
		context,
		9999,
		"test.edu/my_object",
		constants.TypeObject,
	)
	assert.NotNil(t, manager)
	assert.Equal(t, context, manager.Context)
	assert.Equal(t, 9999, manager.WorkItemID)
	assert.Equal(t, "test.edu/my_object", manager.Identifier)
	assert.Equal(t, constants.TypeObject, manager.ItemType)
}

func TestRun_SingleFile(t *testing.T) {
	context := common.NewContext()
	prepareForTest(t, context)
}

func TestRun_Object(t *testing.T) {

}

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
		client := context.S3Clients[target.Provider]
		_, err := client.FPutObject(
			target.Bucket,
			filename,
			pathToFile,
			minio.PutObjectOptions{},
		)
		require.Nil(t, err)

		// TODO: Add StorageRecordSave method to PharosClient
		// and save this record.
		storageRecord := &registry.StorageRecord{
			URL: target.URLFor(filename),
		}
		resp := context.PharosClient.StorageRecordSave(storageRecord, gfIdentifier)
		require.Nil(t, resp.Error)
	}
}
