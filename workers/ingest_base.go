package workers

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"github.com/nsqio/go-nsq"
)

// IngestBase contains the fundamental structures common to all workers.
type IngestBase struct {

	// Context contains info about the context in which the worker is
	// operation, including connections to NSQ, Redis, Pharos, and S3.
	Context *common.Context

	// ItemsInProcess keeps track of WorkItem ids that the worker is
	// currently processing. We need to do this because NSQ does not
	// dedupe messages, so the worker must.
	ItemsInProcess *service.RingList

	// NSQTopic is the name of the NSQ topic to which this worker should
	// subscribe to receive its tasks. The topic names are listed in
	// constants.
	NSQTopic string

	// PreProcessChannel runs checks to ensure that IngestItem should be
	// processed. Since NSQ does not de-dupe messages, the workers must
	// do this themselves.
	PreProcessChannel chan *IngestItem

	// ProcessChannel is where the work actually happens: validation,
	// storage, recording, etc., depending on the worker's responsibility.
	ProcessChannel chan *IngestItem

	// PostProcessChannel is for updating Pharos and NSQ on the status
	// of work. Successfully completed tasks are passed on to the next
	// NSQ topic. Unsuccessful tasks are requeued or sent straight to
	// the cleanup topic. The WorkItem is updated in Pharos with info
	// about its current state and stage.
	PostProcessChannel chan *IngestItem
}

// NewIngestBase creates a new IngestBase worker. Param context is a
// Context object with connections to S3, Redis, Pharos, and NSQ.
// Param bufSize describes the size of the queue buffers. The values
// for opnames/topics are listed in constants.IngestOpNames.
func NewIngestBase(_context *common.Context, bufSize int, nsqTopic string) IngestBase {
	return IngestBase{
		Context:            _context,
		NSQTopic:           nsqTopic,
		ItemsInProcess:     service.NewRingList(bufSize),
		PreProcessChannel:  make(chan *IngestItem, bufSize),
		ProcessChannel:     make(chan *IngestItem, bufSize),
		PostProcessChannel: make(chan *IngestItem, bufSize),
	}
}

func (b *IngestBase) HandleMessage(message *nsq.Message) error {
	// Get the WorkItem from Pharos. If we can't, it's fatal.
	workItem, procErr := b.GetWorkItem(message)
	if procErr != nil && procErr.IsFatal {
		b.Context.Logger.Error(procErr.Error())
		return fmt.Errorf(procErr.Error())
	}

	// ------------------------------------------------------
	//
	// START HERE
	//
	// TODO: If workItem.Retry is false, reject with proper note.

	// Note that returning nil tells NSQ that a worker is
	// working on this item, even if it's not us. We don't
	// want to requeue duplicates, and we don't want to return
	// an error, because that's equivalent to FIN/failed.
	if b.OtherWorkerIsHandlingThis(workItem) {
		return nil
	}
	if b.ImAlreadyProcessingThis(workItem) {
		return nil
	}

	// TODO: Implement this and set WorkItem.Status to "Suspended"
	// if older version is still ingesting.
	// if b.StillIngestingOlderVersion(workItem) {
	// 	return nil
	// }

	// This is a tricky case. We need to be sure NOT to delete
	// the newer item from receiving.
	if b.ShouldAbandonForNewerVersion(workItem) {
		b.PushToQueue(workItem, constants.IngestCleanup)
	}

	// TODO: Also check SupersededByNewerRequest
	// TODO: Check if older version is in progress
	//       and set status to constants.StatusSuspended

	// ------------------------------------------------------

	ingestItem := &IngestItem{
		NSQMessage: message,
		WorkResult: b.GetWorkResult(workItem.ID),
		WorkItem:   workItem,
	}

	// Should we automatically reject the item if it has fatal
	// errors from the prior work attempt? If so, check before
	// resetting.
	ingestItem.WorkResult.Reset()
	ingestItem.WorkResult.Attempt++
	ingestItem.WorkResult.Host, _ = os.Hostname()
	ingestItem.WorkResult.Pid = os.Getpid()

	b.PreProcessChannel <- ingestItem

	return nil
}

// GetWorkItem returns the WorkItem we should be working on.
func (b *IngestBase) GetWorkItem(message *nsq.Message) (*registry.WorkItem, *service.ProcessingError) {
	msgBody := strings.TrimSpace(string(message.Body))
	b.Context.Logger.Info("NSQ Message body: '%s'", msgBody)
	workItemID, err := strconv.Atoi(string(msgBody))
	if err != nil || workItemID == 0 {
		fullErr := fmt.Errorf("Could not get WorkItemId from NSQ message body: %v", err)
		return nil, b.Error(0, msgBody, fullErr, false)
	}
	resp := b.Context.PharosClient.WorkItemGet(workItemID)
	if resp.Error != nil {
		fullErr := fmt.Errorf("Error getting WorkItem %d from Pharos: %v", workItemID, resp.Error)
		return nil, b.Error(workItemID, msgBody, fullErr, false)
	}
	workItem := resp.WorkItem()
	if workItem == nil {
		fullErr := fmt.Errorf("Pharos returned nil for WorkItem %d", workItemID)
		return nil, b.Error(workItemID, msgBody, fullErr, true)
	}
	return workItem, nil
}

// Error creates a new ProcessingError.
func (b *IngestBase) Error(workItemID int, identifier string, err error, isFatal bool) *service.ProcessingError {
	return service.NewProcessingError(
		workItemID,
		identifier,
		err.Error(),
		isFatal,
	)
}

// GetWorkResult returns an WorkResult object for this WorkItem. If one
// already exists in Redis, it returns that. If not, it creates a new one.
func (b *IngestBase) GetWorkResult(workItemID int) *service.WorkResult {
	workResult, err := b.Context.RedisClient.WorkResultGet(workItemID, b.NSQTopic)
	if err != nil {
		b.Context.Logger.Info("No WorkResult in Redis for WorkItem %d. Creating a new one.", workItemID)
		workResult = service.NewWorkResult(b.NSQTopic)
	}
	return workResult
}

// SaveWorkResult saves a WorkResult to Redis and logs an error if any occurs.
// Will try three times, in case Redis is busy.
func (b *IngestBase) SaveWorkResult(workItemID int, result *service.WorkResult) {
	for i := 0; i < 3; i++ {
		err := b.Context.RedisClient.WorkResultSave(workItemID, result)
		if err == nil {
			break
		}
		if i == 2 {
			b.Context.Logger.Info("Error saving WorkResult for WorkItem %d: %v", workItemID, err)
		}
		time.Sleep(time.Duration(250) * time.Millisecond)
	}
}

// SaveWorkItem saves a WorkItem back to Pharos.
func (b *IngestBase) SaveWorkItem(workItem *registry.WorkItem) {
	resp := b.Context.PharosClient.WorkItemSave(workItem)
	if resp.Error != nil {
		b.Context.Logger.Error("Error saving WorkItem %d to Pharos: %v",
			workItem.ID, resp.Error)
	}
}

// FindRelatedWorkItems finds WorkItems with the same action and bagname
// as param WorkItem that have not completed processing.
func (b *IngestBase) FindOtherIngestRequests(workItem *registry.WorkItem) []*registry.WorkItem {
	v := url.Values{}
	v.Add("per_page", "20")
	v.Add("name", workItem.Name)
	v.Add("item_action", constants.ActionIngest)
	v.Add("sort", "date") // Pharos changes this to 'date desc'
	resp := b.Context.PharosClient.WorkItemList(v)
	if resp.Error != nil {
		b.Context.Logger.Error("Error getting WorkItems list from Pharos: %v",
			resp.Error)
	}
	return resp.WorkItems()
}

// FindNewerIngestRequest returns an ingest WorkItem newer than WorkItem
// whose ETag differs. If this exists (and it usually doesn't), it means
// the depositor uploaded a newer version of the bag and we should ingest
// that version instead of the one pointed to by the older WorkItem. (In
// fact, the newer tar file has overwritten the older one in the depositor's
// receiving bucket.)
func (b *IngestBase) FindNewerIngestRequest(workItem *registry.WorkItem) *registry.WorkItem {
	items := b.FindOtherIngestRequests(workItem)
	for _, item := range items {
		if item.Date.After(workItem.Date) && item.ETag != workItem.ETag && !item.ProcessingHasCompleted() {
			return item
		}
	}
	return nil
}

func (b *IngestBase) SupersededByNewerRequest(workItem *registry.WorkItem) bool {
	newerWorkItem := b.FindNewerIngestRequest(workItem)
	if newerWorkItem != nil {
		b.Context.Logger.Info("Skipping WorkItem %d because a newer version of this bag is waiting to be ingested in WorkItem %d", workItem.ID, newerWorkItem.ID)
		return true
	}
	return false
}

// OtherWorkerIsHandlingThis returns true if some other worker is already
// processing this message. This happens often with large ingests that
// take longer to process than NSQ's maximum allowed timeout.
func (b *IngestBase) OtherWorkerIsHandlingThis(workItem *registry.WorkItem) bool {
	hostname, _ := os.Hostname()
	if workItem.Node != hostname || workItem.Pid != os.Getpid() {
		b.Context.Logger.Info("Skipping WorkItem %d because it's being processed by host %s, pid %d", workItem.ID, workItem.Node, workItem.Pid)
		return true
	}
	return false
}

// ImAlreadyProcessingThis returns true and logs a message if this WorkItem
// is already being processed by this worker. This happens with large bags
// when NSQ thinks the item has timed out and tries to reassign it to a new
// worker.
func (b *IngestBase) ImAlreadyProcessingThis(workItem *registry.WorkItem) bool {
	if b.ItemsInProcess.Contains(strconv.Itoa(workItem.ID)) {
		b.Context.Logger.Info("Skipping WorkItem %d because this worker is already working on it host %s, pid %d", workItem.ID, workItem.Node, workItem.Pid)
		return true
	}
	return false
}

// ShouldAbandonForNewerVersion returns true and logs a message
// if the bag in the depositor's receiving bucket was altered
// after the WorkItem was created. In those cases, we typically want to
// stop ingesting the current bag, cancel the WorkItem, and get to work
// on the new bag. The exception is when we've reached the storage phase.
// At that point, we're committed. We should complete the current ingest
// and then process the new one as an update.
func (b *IngestBase) ShouldAbandonForNewerVersion(workItem *registry.WorkItem) bool {
	isLateStageOfIngest := util.StringListContains(constants.LateStagesOfIngest, b.NSQTopic)
	if !isLateStageOfIngest {
		objInfo, err := b.Context.S3StatObject(
			constants.StorageProviderAWS,
			workItem.Bucket,
			workItem.Name)
		if err != nil {
			if strings.Contains(err.Error(), "key does not exist") {
				b.Context.Logger.Info("Stopping work on WorkItem %s because bag %s was deleted from %s", workItem.ID, workItem.Name, workItem.Bucket)
				return true
			}
			// This should never happen, due to checks at startup that
			// panic if provider is missing.
			if strings.Contains(err.Error(), "No S3 client for provider") {
				b.Context.Logger.Error("Can't check S3 for %s/%s because there's no S3 provider for %s", workItem.Bucket, workItem.Name, constants.StorageProviderAWS)
				return true
			}
		} else {
			// No error. We should have objInfo
			if objInfo.ETag != "" && objInfo.ETag != workItem.ETag {
				b.Context.Logger.Info("Stopping work on WorkItem %s because a newer version of bag %s was found in %s", workItem.ID, workItem.Name, workItem.Bucket)
				return true
			}
		}
	}
	return false
}

// Push to queue pushes the specified WorkItem to the named nsqTopic.
func (b *IngestBase) PushToQueue(workItem *registry.WorkItem, nsqTopic string) {
	err := b.Context.NSQClient.Enqueue(
		nsqTopic,
		workItem.ID)
	if err != nil {
		msg := fmt.Sprintf("Error adding WorkItem %d (%s/%s) to NSQ topic %s: %v",
			workItem.ID, workItem.Bucket, workItem.Name, nsqTopic, err)
		b.Context.Logger.Errorf(msg)
		workItem.Note = msg
		b.SaveWorkItem(workItem)
	}
}
