// +build integration

package workers_test

import (
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/APTrust/preservation-services/workers"
	"github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO: Tests... Use mock services for this?

var keyToGoodBag = "example.edu.tagsample_good.tar"
var pathToGoodBag = testutil.PathToUnitTestBag(keyToGoodBag)
var goodbagMd5 = "f4323e5e631834c50d077fc3e03c2fed"
var goodbagSize = int64(40960)
var workItemID = 0

func putBagInS3(t *testing.T, context *common.Context, key, pathToBagFile string) {
	_, err := context.S3Clients[constants.StorageProviderAWS].FPutObject(
		constants.TestBucketReceiving,
		key,
		pathToBagFile,
		minio.PutObjectOptions{})
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	require.Nil(t, err, msg)
}

func putWorkItemInPharos(t *testing.T, context *common.Context, workItem *registry.WorkItem) int {
	resp := context.PharosClient.WorkItemSave(workItem)
	require.Nil(t, resp.Error)
	return resp.WorkItem().ID
}

func queueWorkItem(t *testing.T, context *common.Context, workItemID int) {
	err := context.NSQClient.Enqueue(constants.IngestPreFetch, workItemID)
	require.Nil(t, err)
}

func doSetup(t *testing.T, context *common.Context, key, pathToBagFile string) int {
	if workItemID == 0 {
		putBagInS3(t, context, key, pathToBagFile)
		inst := context.PharosClient.InstitutionGet("test.edu").Institution()
		require.NotNil(t, inst)
		workItem := &registry.WorkItem{
			Action:        constants.ActionIngest,
			BagDate:       testutil.Bloomsday,
			Bucket:        constants.TestBucketReceiving,
			CreatedAt:     testutil.Bloomsday,
			Date:          testutil.Bloomsday,
			ETag:          goodbagMd5,
			InstitutionID: inst.ID,
			Name:          key,
			Retry:         true,
			Size:          goodbagSize,
			Stage:         constants.StageReceive,
			Status:        constants.StatusPending,
		}
		workItemID = putWorkItemInPharos(t, context, workItem)
		queueWorkItem(t, context, workItemID)
	}
	return workItemID
}

func createMetadataGatherer(context *common.Context, workItemID int, ingestObject *service.IngestObject) ingest.Runnable {
	return ingest.NewMetadataGatherer(context, workItemID, ingestObject)
}

func TestNewIngestBase(t *testing.T) {
	var bufSize = 20
	ingestBase := workers.NewIngestBase(
		common.NewContext(),
		createMetadataGatherer,
		bufSize,
		constants.IngestPreFetch,
	)
	assert.Equal(t, bufSize, ingestBase.BufferSize)
	assert.NotNil(t, ingestBase.Context)
	assert.Equal(t, constants.IngestPreFetch+"_worker_chan", ingestBase.NSQChannel)
	assert.Equal(t, constants.IngestPreFetch, ingestBase.NSQTopic)
	assert.NotNil(t, ingestBase.ItemsInProcess)
	assert.NotNil(t, ingestBase.ProcessChannel)
	assert.NotNil(t, ingestBase.SuccessChannel)
	assert.NotNil(t, ingestBase.ErrorChannel)
	assert.NotNil(t, ingestBase.FatalErrorChannel)
}

func TestIngestBase_HandleMessage(t *testing.T) {

}

func TestIngestBase_GetWorkItem(t *testing.T) {

}

func TestIngestBase_Error(t *testing.T) {

}

func TestIngestBase_ShouldSkipThis(t *testing.T) {

}

func TestIngestBase_GetWorkResult(t *testing.T) {

}

func TestIngestBase_SaveWorkResult(t *testing.T) {

}

func TestIngestBase_SaveWorkItem(t *testing.T) {

}

func TestIngestBase_FindOtherIngestRequests(t *testing.T) {

}

func TestIngestBase_FindNewerIngestRequest(t *testing.T) {

}

func TestIngestBase_StillIngestingOlderVersion(t *testing.T) {

}

func TestIngestBase_SupersededByNewerRequest(t *testing.T) {

}

func TestIngestBase_OtherWorkerIsHandlingThis(t *testing.T) {

}

func TestIngestBase_ImAlreadyProcessingThis(t *testing.T) {

}

func TestIngestBase_IsLateStageOfIngest(t *testing.T) {

}

func TestIngestBase_ShouldAbandonForNewerVersion(t *testing.T) {

}

func TestIngestBase_MarkAsStarted(t *testing.T) {

}

func TestIngestBase_PushToQueue(t *testing.T) {

}
