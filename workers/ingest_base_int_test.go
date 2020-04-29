// +build integration

package workers_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

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

var keyToGoodBag = "example.edu.tagsample_good.tar"
var pathToGoodBag = testutil.PathToUnitTestBag(keyToGoodBag)
var goodbagMd5 = "f4323e5e631834c50d077fc3e03c2fed"
var goodbagSize = int64(40960)
var objectIdentifier = "test.edu/example.edu.tagsample_good"
var bufSize = 20
var testWorkItem *registry.WorkItem
var olderWorkItemID = 0
var olderStillIngestingID = 0
var newerWorkItemID = 0
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

func putWorkItemInPharos(t *testing.T, context *common.Context, workItem *registry.WorkItem) *registry.WorkItem {
	resp := context.PharosClient.WorkItemSave(workItem)
	require.Nil(t, resp.Error)
	require.NotNil(t, resp.WorkItem())
	return resp.WorkItem()
}

func saveSimilarWorkItems(t *testing.T, context *common.Context, workItem *registry.WorkItem) {
	// Older ingest request. Not started.
	olderDate := workItem.BagDate.Add(time.Hour * -6)
	olderIngestRequest := copyWorkItem(t, workItem)
	olderIngestRequest.BagDate = olderDate
	olderIngestRequest.Date = olderDate
	olderIngestRequest.CreatedAt = olderDate
	olderIngestRequest.UpdatedAt = olderDate
	resp := context.PharosClient.WorkItemSave(olderIngestRequest)
	require.Nil(t, resp.Error)
	olderWorkItemID = resp.WorkItem().ID

	// Older ingest request. Still in process.
	olderDate = workItem.BagDate.Add(time.Hour * -3)
	olderStillIngesting := copyWorkItem(t, workItem)
	olderStillIngesting.BagDate = olderDate
	olderStillIngesting.Date = olderDate
	olderStillIngesting.CreatedAt = olderDate
	olderStillIngesting.UpdatedAt = olderDate
	olderStillIngesting.Stage = constants.StageStore
	olderStillIngesting.Status = constants.StatusStarted
	resp = context.PharosClient.WorkItemSave(olderStillIngesting)
	require.Nil(t, resp.Error)
	olderStillIngestingID = resp.WorkItem().ID

	// Newer ingest request. Not started. Newer ETag.
	newerDate := workItem.BagDate.Add(time.Hour * 6)
	newerIngestRequest := copyWorkItem(t, workItem)
	newerIngestRequest.BagDate = newerDate
	newerIngestRequest.Date = newerDate
	newerIngestRequest.CreatedAt = newerDate
	newerIngestRequest.UpdatedAt = newerDate
	newerIngestRequest.ETag = "12345678"
	resp = context.PharosClient.WorkItemSave(newerIngestRequest)
	require.Nil(t, resp.Error)
	newerWorkItemID = resp.WorkItem().ID
}

func copyWorkItem(t *testing.T, workItem *registry.WorkItem) *registry.WorkItem {
	data, err := workItem.ToJSON()
	require.Nil(t, err)
	item, err := registry.WorkItemFromJSON(data)
	require.Nil(t, err)
	item.ID = 0
	return item
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

func putWorkResultInRedis(t *testing.T, context *common.Context, workItem *registry.WorkItem) {
	workResult := service.NewWorkResult(constants.IngestPreFetch)
	err := context.RedisClient.WorkResultSave(workItem.ID, workResult)
	require.Nil(t, err)
}
func queueWorkItem(t *testing.T, context *common.Context, workItemID int) {
	err := context.NSQClient.Enqueue(constants.IngestPreFetch, workItemID)
	require.Nil(t, err)
}

func doSetup(t *testing.T, key, pathToBagFile string) int {
	if testWorkItem == nil {
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
		testWorkItem = putWorkItemInPharos(t, context, workItem)
		msgBody := []byte(strconv.Itoa(testWorkItem.ID))
		var msgId [16]byte
		copy(msgId[:], []byte("9999"))
		copyOfNsqMessage = nsq.NewMessage(msgId, msgBody)
		saveSimilarWorkItems(t, context, testWorkItem)
		putIngestObjectInRedis(t, context, testWorkItem)
		putWorkResultInRedis(t, context, testWorkItem)
		queueWorkItem(t, context, testWorkItem.ID)
	}
	return testWorkItem.ID
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
	require.NotNil(t, 0, testWorkItem)
	require.NotNil(t, copyOfNsqMessage)
	ingestBase := getIngestBase()
	workItem, err := ingestBase.GetWorkItem(copyOfNsqMessage)
	assert.Nil(t, err)
	require.NotNil(t, workItem)
	assert.Equal(t, testWorkItem.ID, workItem.ID)
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
	ingestObj, err := ingestBase.GetIngestObject(testWorkItem)
	assert.Nil(t, err)
	require.NotNil(t, ingestObj)
	assert.Equal(t, objectIdentifier, ingestObj.Identifier())
}

func TestIngestBase_GetWorkResult(t *testing.T) {
	ingestBase := getIngestBase()
	workResult := ingestBase.GetWorkResult(testWorkItem.ID)
	assert.NotNil(t, workResult)
}

func TestIngestBase_SaveWorkResult(t *testing.T) {
	ingestBase := getIngestBase()
	workResult := ingestBase.GetWorkResult(testWorkItem.ID)
	require.NotNil(t, workResult)
	err := ingestBase.SaveWorkResult(testWorkItem.ID, workResult)
	assert.Nil(t, err)
}

func TestIngestBase_SaveWorkItem(t *testing.T) {
	ingestBase := getIngestBase()
	err := ingestBase.SaveWorkItem(testWorkItem)
	assert.Nil(t, err)
}

func TestIngestBase_FindOtherIngestRequests(t *testing.T) {
	doSetup(t, keyToGoodBag, pathToGoodBag)
	ingestBase := getIngestBase()

	// Should find the WorkItem itself and the other three
	// similar WorkItems we added in doSetup
	otherWorkItems := ingestBase.FindOtherIngestRequests(testWorkItem)
	require.Equal(t, 4, len(otherWorkItems))
}

func TestIngestBase_FindNewerIngestRequest(t *testing.T) {
	doSetup(t, keyToGoodBag, pathToGoodBag)
	ingestBase := getIngestBase()

	newerItem := ingestBase.FindNewerIngestRequest(testWorkItem)
	require.NotNil(t, newerItem)
	assert.Equal(t, newerWorkItemID, newerItem.ID)
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
