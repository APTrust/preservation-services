//go:build integration

package workers_test

import (
	ctx "context"
	"fmt"
	"os"
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
	"github.com/minio/minio-go/v7"
	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var keyToGoodBag = "example.edu.tagsample_good.tar"
var pathToGoodBag = testutil.PathToUnitTestBag(keyToGoodBag)
var goodbagETag = "00000000888888880000000077777777"
var goodbagSize = int64(40960)
var objectIdentifier = "test.edu/example.edu.tagsample_good"
var bufSize = 20
var testWorkItem *registry.WorkItem
var olderWorkItemID = int64(0)
var olderStillIngestingID = int64(0)
var newerWorkItemID = int64(0)
var copyOfNsqMessage *nsq.Message
var testInstitution *registry.Institution

func putBagInS3(t *testing.T, context *common.Context, key, pathToBagFile string) {
	client := context.S3Clients[constants.StorageProviderAWS]
	_, err := client.FPutObject(
		ctx.Background(),
		constants.TestBucketReceiving,
		key,
		pathToBagFile,
		minio.PutObjectOptions{})
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	require.Nil(t, err, msg)

	objInfo, err := client.StatObject(ctx.Background(), constants.TestBucketReceiving, key, minio.StatObjectOptions{})
	require.Nil(t, err)
	goodbagETag = objInfo.ETag
}

func putWorkItemInRegistry(t *testing.T, context *common.Context, workItem *registry.WorkItem) *registry.WorkItem {
	resp := context.RegistryClient.WorkItemSave(workItem)
	require.Nil(t, resp.Error)
	require.NotNil(t, resp.WorkItem())
	return resp.WorkItem()
}

func saveSimilarWorkItems(t *testing.T, context *common.Context, workItem *registry.WorkItem) {
	// Older ingest request. Not started.
	olderDate := workItem.BagDate.Add(time.Hour * -6)
	olderIngestRequest := copyWorkItem(t, workItem)
	olderIngestRequest.BagDate = olderDate
	olderIngestRequest.DateProcessed = olderDate
	olderIngestRequest.CreatedAt = olderDate
	olderIngestRequest.UpdatedAt = olderDate
	resp := context.RegistryClient.WorkItemSave(olderIngestRequest)
	require.Nil(t, resp.Error)
	olderWorkItemID = resp.WorkItem().ID

	// Older ingest request. Still in process.
	olderDate = workItem.BagDate.Add(time.Hour * -3)
	olderStillIngesting := copyWorkItem(t, workItem)
	olderStillIngesting.BagDate = olderDate
	olderStillIngesting.DateProcessed = olderDate
	olderStillIngesting.CreatedAt = olderDate
	olderStillIngesting.UpdatedAt = olderDate
	olderStillIngesting.Stage = constants.StageStore
	olderStillIngesting.Status = constants.StatusStarted
	resp = context.RegistryClient.WorkItemSave(olderStillIngesting)
	require.Nil(t, resp.Error)
	olderStillIngestingID = resp.WorkItem().ID

	// Newer ingest request. Not started. Newer ETag.
	newerDate := workItem.BagDate.Add(time.Hour * 6)
	newerIngestRequest := copyWorkItem(t, workItem)
	newerIngestRequest.BagDate = newerDate
	newerIngestRequest.DateProcessed = newerDate
	newerIngestRequest.CreatedAt = newerDate
	newerIngestRequest.UpdatedAt = newerDate
	newerIngestRequest.ETag = "12345678123456781234567812345678"
	resp = context.RegistryClient.WorkItemSave(newerIngestRequest)
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
func queueWorkItem(t *testing.T, context *common.Context, workItemID int64) {
	err := context.NSQClient.Enqueue(constants.IngestPreFetch, workItemID)
	require.Nil(t, err)
}

func doSetup(t *testing.T, key, pathToBagFile string) int64 {
	if testWorkItem == nil {
		context := common.NewContext()
		putBagInS3(t, context, key, pathToBagFile)
		testInstitution = context.RegistryClient.InstitutionByIdentifier("test.edu").Institution()
		require.NotNil(t, testInstitution)
		hostname, _ := os.Hostname()
		workItem := &registry.WorkItem{
			Action:           constants.ActionIngest,
			BagDate:          testutil.Bloomsday,
			Bucket:           constants.TestBucketReceiving,
			CreatedAt:        testutil.Bloomsday,
			DateProcessed:    testutil.Bloomsday,
			ETag:             goodbagETag,
			InstitutionID:    testInstitution.ID,
			Name:             key,
			Node:             hostname,
			Note:             "Item is awaiting ingest",
			ObjectIdentifier: objectIdentifier,
			Outcome:          constants.StatusPending,
			Pid:              os.Getpid(),
			Retry:            true,
			Size:             goodbagSize,
			Stage:            constants.StageReceive,
			Status:           constants.StatusPending,
			User:             "system@aptrust.org",
		}
		testWorkItem = putWorkItemInRegistry(t, context, workItem)
		assert.NotNil(t, testWorkItem)
		assert.NotEmpty(t, testWorkItem.ID)
		msgBody := []byte(strconv.FormatInt(testWorkItem.ID, 10))
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

func createMetadataGatherer(context *common.Context, workItemID int64, ingestObject *service.IngestObject) ingest.Runnable {
	return ingest.NewMetadataGatherer(context, workItemID, ingestObject)
}

func getIngestBase() *workers.IngestBase {
	settings := &workers.Settings{
		ChannelBufferSize:                         20,
		DeleteFromReceivingAfterFatalError:        false,
		DeleteFromReceivingAfterMaxFailedAttempts: false,
		MaxAttempts:                         3,
		NSQChannel:                          constants.IngestPreFetch + "_worker_chan",
		NSQTopic:                            constants.IngestPreFetch,
		NextQueueTopic:                      constants.IngestValidation,
		NextWorkItemStage:                   constants.StageValidate,
		NumberOfWorkers:                     2,
		PushToCleanupAfterMaxFailedAttempts: false,
		PushToCleanupOnFatalError:           false,
		RequeueTimeout:                      (20 * time.Second),
		WorkItemSuccessNote:                 "Finished pre-fetch metadata gathering",
	}
	return workers.NewIngestBase(common.NewContext(), createMetadataGatherer, settings)
}

func TestNewIngestBase(t *testing.T) {
	ingestBase := getIngestBase()
	assert.Equal(t, bufSize, ingestBase.Settings.ChannelBufferSize)
	assert.NotNil(t, ingestBase.Context)
	assert.Equal(t, constants.IngestPreFetch+"_worker_chan", ingestBase.Settings.NSQChannel)
	assert.Equal(t, constants.IngestPreFetch, ingestBase.Settings.NSQTopic)
	assert.NotNil(t, ingestBase.ItemsInProcess)
	assert.NotNil(t, ingestBase.ProcessChannel)
	assert.NotNil(t, ingestBase.SuccessChannel)
	assert.NotNil(t, ingestBase.ErrorChannel)
	assert.NotNil(t, ingestBase.FatalErrorChannel)
}

// TODO: Test this for different logic cases.
// All the methods it calls are already tested.
func TestIngestBase_HandleMessage(t *testing.T) {
	//doSetup(t, keyToGoodBag, pathToGoodBag)
	//ingestBase := getIngestBase()
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
	assert.Equal(t, int64(999), processingErr.WorkItemID)
	assert.Equal(t, "my-identifier", processingErr.Identifier)
	assert.Equal(t, "This is the internal error", processingErr.Message)
	assert.False(t, processingErr.IsFatal)

	processingErr = ingestBase.Error(999, "my-identifier", err, true)
	assert.True(t, processingErr.IsFatal)
}

// TODO: This should be broken out into true/false tests
// for each condition.
func TestIngestBase_ShouldSkipThis(t *testing.T) {
	doSetup(t, keyToGoodBag, pathToGoodBag)
	ingestBase := getIngestBase()

	copyOfWorkItem := copyWorkItem(t, testWorkItem)
	assert.True(t, ingestBase.ShouldSkipThis(copyOfWorkItem))
}

func TestIngestBase_GetInstitutionIdentifier(t *testing.T) {
	ingestBase := getIngestBase()
	identifier, err := ingestBase.GetInstitutionIdentifier(testInstitution.ID)
	assert.Nil(t, err)
	assert.Equal(t, "test.edu", identifier)
}

func TestIngestBase_IngestObjectGet(t *testing.T) {
	ingestBase := getIngestBase()
	ingestObj, err := ingestBase.IngestObjectGet(testWorkItem)
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
	doSetup(t, keyToGoodBag, pathToGoodBag)
	ingestBase := getIngestBase()

	// Should be true because of WorkItem olderStillIngestingID
	assert.True(t, ingestBase.StillIngestingOlderVersion(testWorkItem))
}

func TestIngestBase_SupersededByNewerRequest(t *testing.T) {
	doSetup(t, keyToGoodBag, pathToGoodBag)
	ingestBase := getIngestBase()

	// Should be true because of WorkItem newerWorkItemID
	assert.True(t, ingestBase.SupersededByNewerRequest(testWorkItem))
}

func TestIngestBase_OtherWorkerIsHandlingThis(t *testing.T) {
	ingestBase := getIngestBase()
	hostname, _ := os.Hostname()

	// False because hostname and pid match ours
	assert.False(t, ingestBase.OtherWorkerIsHandlingThis(testWorkItem))

	// True because of pid mismatch
	item := copyWorkItem(t, testWorkItem)
	item.Node = hostname
	item.Pid = 99999999
	assert.True(t, ingestBase.OtherWorkerIsHandlingThis(item))

	// True because of hostname mismatch
	item = copyWorkItem(t, testWorkItem)
	item.Node = "......"
	item.Pid = os.Getpid()
	assert.True(t, ingestBase.OtherWorkerIsHandlingThis(item))
}

func TestIngestBase_ImAlreadyProcessingThis(t *testing.T) {
	ingestBase := getIngestBase()

	// False because ItemsInProcess is empty
	assert.False(t, ingestBase.ImAlreadyProcessingThis(testWorkItem))

	// True because WorkItem.ID is now in ItemsInProcess
	ingestBase.ItemsInProcess.Add(strconv.FormatInt(testWorkItem.ID, 10))
	assert.True(t, ingestBase.ImAlreadyProcessingThis(testWorkItem))
}

func TestIngestBase_Add_Remove_InProcessList(t *testing.T) {
	ingestBase := getIngestBase()

	ingestBase.AddToInProcessList(1234)
	assert.True(t, ingestBase.ItemsInProcess.Contains("1234"))

	ingestBase.RemoveFromInProcessList(1234)
	assert.False(t, ingestBase.ItemsInProcess.Contains("1234"))
}

func TestIngestBase_IsLateStageOfIngest(t *testing.T) {
	ingestBase := getIngestBase()
	earlyStages := []string{
		constants.IngestPreFetch,
		constants.IngestValidation,
		constants.IngestReingestCheck,
		constants.IngestStaging,
		constants.IngestFormatIdentification,
	}
	lateStages := []string{
		constants.IngestStorage,
		constants.IngestStorageValidation,
		constants.IngestRecord,
		constants.IngestCleanup,
	}
	for _, stage := range earlyStages {
		ingestBase.Settings.NSQTopic = stage
		assert.False(t, ingestBase.IsLateStageOfIngest())
	}
	for _, stage := range lateStages {
		ingestBase.Settings.NSQTopic = stage
		assert.True(t, ingestBase.IsLateStageOfIngest())
	}
}

func TestIngestBase_ShouldAbandonForNewerVersion(t *testing.T) {
	doSetup(t, keyToGoodBag, pathToGoodBag)
	ingestBase := getIngestBase()
	item := copyWorkItem(t, testWorkItem)

	// False because ETag of item in S3 receving matches
	// ETag of WorkItem
	assert.False(t, ingestBase.ShouldAbandonForNewerVersion(item))

	// True, because ETag of S3 item no longer matches
	item.ETag = "12341234123412341234123412341234"
	assert.True(t, ingestBase.ShouldAbandonForNewerVersion(item))

	// False, because even though ETag no longer matches,
	// we too far into the ingest process to turn back.
	ingestBase.Settings.NSQTopic = constants.IngestStorage
	assert.False(t, ingestBase.ShouldAbandonForNewerVersion(item))
}

func TestIngestBase_MarkAsStarted(t *testing.T) {
	doSetup(t, keyToGoodBag, pathToGoodBag)
	ingestBase := getIngestBase()

	copyOfWorkItem := copyWorkItem(t, testWorkItem)
	putWorkItemInRegistry(t, ingestBase.Context, copyOfWorkItem)

	hostname, _ := os.Hostname()
	task := &workers.Task{
		NSQMessage: copyOfNsqMessage,
		WorkResult: service.NewWorkResult(ingestBase.Settings.NSQTopic),
		WorkItem:   copyOfWorkItem,
	}

	//data, _ := task.WorkItem.ToJSON()
	//fmt.Println(string(data))

	assert.NotEqual(t, constants.StatusStarted, task.WorkItem.Status)
	assert.Equal(t, 0, task.WorkResult.Attempt)

	ingestBase.MarkAsStarted(task)
	assert.Equal(t, constants.StatusStarted, task.WorkItem.Status)
	assert.Equal(t, "Item has started stage ingest01_prefetch", task.WorkItem.Note)
	assert.Equal(t, 1, task.WorkResult.Attempt)
	assert.Equal(t, hostname, task.WorkResult.Host)
	assert.Equal(t, os.Getpid(), task.WorkResult.Pid)
	assert.False(t, task.WorkResult.StartedAt.IsZero())
}

func TestIngestBase_FinishItem(t *testing.T) {
	doSetup(t, keyToGoodBag, pathToGoodBag)
	ingestBase := getIngestBase()

	copyOfWorkItem := putWorkItemInRegistry(t, ingestBase.Context, copyWorkItem(t, testWorkItem))

	msgBody := []byte(strconv.FormatInt(copyOfWorkItem.ID, 10))
	var msgId [16]byte
	copy(msgId[:], []byte("3333"))
	testNSQMessage := nsq.NewMessage(msgId, msgBody)

	task := &workers.Task{
		NSQMessage: testNSQMessage,
		WorkResult: service.NewWorkResult(ingestBase.Settings.NSQTopic),
		WorkItem:   copyOfWorkItem,
	}

	ingestBase.MarkAsStarted(task)
	ingestBase.AddToInProcessList(task.WorkItem.ID)

	task.WorkItem.Status = constants.StatusCancelled
	task.WorkItem.Note = "Cancelled for testing purposes"
	task.NextQueueTopic = "finish_test_topic"
	ingestBase.FinishItem(task)

	// Fetch and test WorkItem
	retrievedWorkItem, err := ingestBase.GetWorkItem(testNSQMessage)
	require.Nil(t, err)
	require.NotNil(t, retrievedWorkItem)
	assert.Equal(t, "Cancelled for testing purposes", retrievedWorkItem.Note)
	assert.Equal(t, constants.StatusCancelled, retrievedWorkItem.Status)
	assert.Equal(t, "", retrievedWorkItem.Node)
	assert.Equal(t, 0, retrievedWorkItem.Pid)

	// Fetch and test WorkResult
	retrievedWorkResult := ingestBase.GetWorkResult(task.WorkItem.ID)
	assert.True(t, retrievedWorkResult.Finished())
	assert.Equal(t, 1, retrievedWorkResult.Attempt)

	// Make sure item was removed from ItemsInProcess
	assert.False(t, ingestBase.ItemsInProcess.Contains(strconv.FormatInt(task.WorkItem.ID, 10)))

	// Make sure item was pushed to queue
	stats, procErr := ingestBase.Context.NSQClient.GetStats()
	require.Nil(t, procErr)
	topic := stats.GetTopic("finish_test_topic")
	require.NotNil(t, topic)
	assert.EqualValues(t, 1, topic.MessageCount)
}

func TestIngestBase_PushToQueue(t *testing.T) {
	doSetup(t, keyToGoodBag, pathToGoodBag)
	ingestBase := getIngestBase()
	copyOfWorkItem := copyWorkItem(t, testWorkItem)

	// Push one item into a topic that we create.
	ingestBase.PushToQueue(copyOfWorkItem, "integration_test_topic")

	// Now get the NSQ topic and make sure the item is there.
	// Since we just created this topic, it should have
	// one item in it.
	stats, err := ingestBase.Context.NSQClient.GetStats()
	require.Nil(t, err)
	topic := stats.GetTopic("integration_test_topic")
	require.NotNil(t, topic)
	assert.EqualValues(t, 1, topic.MessageCount)
}
