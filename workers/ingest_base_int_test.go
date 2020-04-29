// +build integration

package workers_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/APTrust/preservation-services/workers"
	"github.com/minio/minio-go/v6"
	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO: Tests... Use mock services for this?

var keyToGoodBag = "example.edu.tagsample_good.tar"
var pathToGoodBag = testutil.PathToUnitTestBag(keyToGoodBag)
var goodbagMd5 = "f4323e5e631834c50d077fc3e03c2fed"
var goodbagSize = int64(40960)
var objectIdentifier = "test.edu/example.edu.tagsample_good"
var bufSize = 20
var workItemID = 0
var copyOfNsqMessage *nsq.Message
var testInstitution *registry.Institution

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

func putIngestObjectInRedis(t *testing.T, context *common.Context, workItem *registry.WorkItem) {
	ingestObject := service.NewIngestObject(
		workItem.Bucket,
		workItem.Name,
		workItem.ETag,
		"test.edu",
		workItem.InstitutionID,
		workItem.Size,
	)
	err := context.RedisClient.IngestObjectSave(workItem.ID, ingestObject)
	require.Nil(t, err)
}

func queueWorkItem(t *testing.T, context *common.Context, workItemID int) {
	err := context.NSQClient.Enqueue(constants.IngestPreFetch, workItemID)
	require.Nil(t, err)
}

func doSetup(t *testing.T, key, pathToBagFile string) int {
	if workItemID == 0 {
		context := common.NewContext()
		putBagInS3(t, context, key, pathToBagFile)
		testInstitution = context.PharosClient.InstitutionGet("test.edu").Institution()
		require.NotNil(t, testInstitution)
		workItem := &registry.WorkItem{
			Action:           constants.ActionIngest,
			BagDate:          testutil.Bloomsday,
			Bucket:           constants.TestBucketReceiving,
			CreatedAt:        testutil.Bloomsday,
			Date:             testutil.Bloomsday,
			ETag:             goodbagMd5,
			InstitutionID:    testInstitution.ID,
			Name:             key,
			Note:             "Item is awaiting ingest",
			ObjectIdentifier: objectIdentifier,
			Outcome:          constants.StatusPending,
			Retry:            true,
			Size:             goodbagSize,
			Stage:            constants.StageReceive,
			Status:           constants.StatusPending,
		}
		workItemID = putWorkItemInPharos(t, context, workItem)
		workItem.ID = workItemID
		putIngestObjectInRedis(t, context, workItem)
		queueWorkItem(t, context, workItemID)
		msgBody := []byte(strconv.Itoa(workItemID))
		var msgId [16]byte
		copy(msgId[:], []byte("9999"))
		copyOfNsqMessage = nsq.NewMessage(msgId, msgBody)
	}
	return workItemID
}

func createMetadataGatherer(context *common.Context, workItemID int, ingestObject *service.IngestObject) ingest.Runnable {
	return ingest.NewMetadataGatherer(context, workItemID, ingestObject)
}

func getIngestBase() *workers.IngestBase {
	return workers.NewIngestBase(
		common.NewContext(),
		createMetadataGatherer,
		bufSize,
		2,
		constants.IngestPreFetch,
	)
}

func TestNewIngestBase(t *testing.T) {
	ingestBase := getIngestBase()
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
	doSetup(t, keyToGoodBag, pathToGoodBag)
}

func TestIngestBase_GetWorkItem(t *testing.T) {
	doSetup(t, keyToGoodBag, pathToGoodBag)
	ingestBase := getIngestBase()
	workItem, err := ingestBase.GetWorkItem(copyOfNsqMessage)
	assert.Nil(t, err)
	require.NotNil(t, workItem)
	assert.Equal(t, workItemID, workItem.ID)
}

func TestIngestBase_Error(t *testing.T) {
	ingestBase := getIngestBase()
	err := fmt.Errorf("This is the internal error")
	processingErr := ingestBase.Error(999, "my-identifier", err, false)
	assert.Equal(t, 999, processingErr.WorkItemID)
	assert.Equal(t, "my-identifier", processingErr.Identifier)
	assert.Equal(t, "This is the internal error", processingErr.Message)
	assert.False(t, processingErr.IsFatal)

	processingErr = ingestBase.Error(999, "my-identifier", err, true)
	assert.True(t, processingErr.IsFatal)
}

func TestIngestBase_ShouldSkipThis(t *testing.T) {

}

func TestIngestBase_GetInstitutionIdentifier(t *testing.T) {
	ingestBase := getIngestBase()
	identifier, err := ingestBase.GetInstitutionIdentifier(testInstitution.ID)
	assert.Nil(t, err)
	assert.Equal(t, "test.edu", identifier)
}

func TestIngestBase_GetIngestObject(t *testing.T) {
	ingestBase := getIngestBase()
	workItem, procErr := ingestBase.GetWorkItem(copyOfNsqMessage)
	fmt.Println("---> WorkItemID:", workItem.ID)
	require.Nil(t, procErr)
	require.NotNil(t, workItem)
	ingestObj, err := ingestBase.GetIngestObject(workItem)
	assert.Nil(t, err)
	require.NotNil(t, ingestObj)
	assert.Equal(t, objectIdentifier, ingestObj.Identifier())
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
