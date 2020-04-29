package workers

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"github.com/nsqio/go-nsq"
)

// IngestBase contains the fundamental structures common to all workers.
type IngestBase struct {

	// BufferSize is the size of the buffer for the PreProcess, Process,
	// and PostProcess channels.
	BufferSize int

	// Context contains info about the context in which the worker is
	// operation, including connections to NSQ, Redis, Pharos, and S3.
	Context *common.Context

	// ItemsInProcess keeps track of WorkItem ids that the worker is
	// currently processing. We need to do this because NSQ does not
	// dedupe messages, so the worker must.
	ItemsInProcess *service.RingList

	// NSQChannel is the name of the NSQ channel to which this worker should
	// subscribe to receive its tasks. The channel is the topic name plus
	// "_worker_chan" Topic names are listed in constants.
	NSQChannel string

	// NSQTopic is the name of the NSQ topic to which this worker should
	// subscribe to receive its tasks. The topic names are listed in
	// constants.
	NSQTopic string

	// ProcessChannel is where the work actually happens: validation,
	// storage, recording, etc., depending on the worker's responsibility.
	ProcessChannel chan *IngestItem

	// SuccessChannel processes items that have gone through the
	// ProcessChannel with no errors.
	SuccessChannel chan *IngestItem

	// ErrorChannel processes items that have gone through the
	// ProcessChannel with one or more non-fatal errors. These items
	// typically should be retried.
	ErrorChannel chan *IngestItem

	// FatalErrorChannel processes items that have gone through the
	// ProcessChannel with one or more fatal errors. These items
	// typically should not be retried.
	FatalErrorChannel chan *IngestItem

	// institutionCache maps institution ids to identifiers. The institution
	// identifier is typically a domain name like "virginia.edu", "test.org",
	// etc.
	institutionCache map[int]string

	// nsqConsumer implements HandleMessage to receive messages from NSQ.
	nsqConsumer *nsq.Consumer

	// processorConstructor is a function that returns an instance of
	// *ingest.Base that will handle the processing for this worker.
	processorConstructor ingest.BaseConstructor
}

// NewIngestBase creates a new IngestBase worker. Param context is a
// Context object with connections to S3, Redis, Pharos, and NSQ.
// Param bufSize describes the size of the queue buffers. The values
// for opnames/topics are listed in constants.IngestOpNames.
func NewIngestBase(context *common.Context, processorConstructor ingest.BaseConstructor, bufSize, numWorkers int, nsqTopic string) *IngestBase {
	base := &IngestBase{
		BufferSize:           bufSize,
		Context:              context,
		NSQChannel:           nsqTopic + "_worker_chan",
		NSQTopic:             nsqTopic,
		ItemsInProcess:       service.NewRingList(bufSize),
		ProcessChannel:       make(chan *IngestItem, bufSize),
		SuccessChannel:       make(chan *IngestItem, bufSize),
		ErrorChannel:         make(chan *IngestItem, bufSize),
		FatalErrorChannel:    make(chan *IngestItem, bufSize),
		processorConstructor: processorConstructor,
		institutionCache:     make(map[int]string),
	}

	// We typically want 2 or so workers to do the heavy,
	// long-running processing invlolved in IngestItem.Processor.Run().
	// Too many workers, however, can be counterproductive,
	// maxing out cpu, memory, and/or network bandwidth. The
	// Success/Error/FatalError channels do lightweight work that
	// usually takes <2 seconds per item, so a single go routine
	// will suffice for those.
	for i := 0; i < numWorkers; i++ {
		go base.processItem()
	}

	return base
}

// RegisterAsNsqConsumer registers this worker as an NSQ consumer on
// IngestBase.NSQTopic and IngestBase.NSQChannel.
func (b *IngestBase) RegisterAsNsqConsumer() error {
	config := nsq.NewConfig()
	// nsqConfig.Set("max_in_flight", 2 * )
	consumer, err := nsq.NewConsumer(b.NSQTopic, b.NSQChannel, config)
	if err != nil {
		return err
	}
	b.nsqConsumer = consumer
	b.nsqConsumer.AddHandler(b)
	b.nsqConsumer.ConnectToNSQLookupd(b.Context.Config.NsqLookupd)
	return nil
}

// HandleMessage checks to see whether we should process this message at
// all. If so, it packages up an IngestItem with everything except the
// Processor object (an instance of ingest.Base). It puts the IngestItem
// in the the PreProcessChannel. From there, the worker should instantiate
// and assign the right IngestItem.Processor type and push the item into
// the ProcessChannel.
func (b *IngestBase) HandleMessage(message *nsq.Message) error {
	// Get the WorkItem from Pharos. If we can't, it's fatal.
	workItem, procErr := b.GetWorkItem(message)
	if procErr != nil && procErr.IsFatal {
		b.Context.Logger.Error(procErr.Error())
		return fmt.Errorf(procErr.Error())
	}

	// If there's any reason to skip this, return nil to tell
	// NSQ it's done. This function also sets some properties on
	// the WorkItem, so admins and depositors will know the item's
	// state. So save the WorkItem before returning.
	if b.ShouldSkipThis(workItem) {
		b.SaveWorkItem(workItem)
		return nil
	}

	workResult := b.GetWorkResult(workItem.ID)
	ingestObject, err := b.GetIngestObject(workItem)
	if err != nil {
		message := fmt.Sprintf("WorkItem %d: %v", workItem.ID, err)
		b.Context.Logger.Error(message)
		workItem.Note = message
		b.SaveWorkItem(workItem)
		workResult.Attempt++
		b.SaveWorkResult(workItem.ID, workResult)
		return err
	}

	// Set up the IngestItem.
	ingestItem := &IngestItem{
		NSQMessage: message,
		WorkResult: workResult,
		WorkItem:   workItem,
		Processor:  b.processorConstructor(b.Context, workItem.ID, ingestObject),
	}

	// Tell Pharos and Redis we're starting work on this
	b.MarkAsStarted(ingestItem)

	// Put the item into the PreProcess channel, which
	// will set up the Processor to handle it.
	b.ProcessChannel <- ingestItem

	// Return nil (no error) so NSQ knows we're working on this.
	return nil
}

// ProcessItem calls ingestItem.Processor.Run() and then routes the
// ingestItem to the SuccessChannel, the ErrorChannel, or the
// FatalErrorChannel, depending on the outcome.
func (b *IngestBase) processItem() {
	for ingestItem := range b.ProcessChannel {
		count, errors := ingestItem.Processor.Run()
		ingestItem.WorkResult.Errors = errors

		b.Context.Logger.Info("WorkItem %d: count %d", ingestItem.WorkItem.ID, count)

		if ingestItem.WorkResult.HasFatalErrors() {
			b.FatalErrorChannel <- ingestItem
		} else if ingestItem.WorkResult.HasErrors() {
			b.ErrorChannel <- ingestItem
		} else {
			b.SuccessChannel <- ingestItem
		}
	}
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

// ShouldSkipThis returns true if there are any reasons not process this
// WorkItem.
func (b *IngestBase) ShouldSkipThis(workItem *registry.WorkItem) bool {

	// It's possible that another worker recently marked this as
	// "do not retry." If that's the case, skip it.
	if workItem.Retry == false {
		message := fmt.Sprintf("Rejecting WorkItem %d because retry = false", workItem.ID)
		workItem.MarkNoLongerInProgress(
			workItem.Stage,
			workItem.Status,
			message,
		)
		b.Context.Logger.Info(message)
		return true
	}

	// Note that returning nil tells NSQ that a worker is
	// working on this item, even if it's not us. We don't
	// want to requeue duplicates, and we don't want to return
	// an error, because that's equivalent to FIN/failed.
	if b.OtherWorkerIsHandlingThis(workItem) {
		return true
	}

	// See if this worker is already processing this item.
	// This happens sometimes when NSQ thinks the item has
	// timed out while a worker is validating or storing
	// an object.
	if b.ImAlreadyProcessingThis(workItem) {
		return true
	}

	// TODO: Implement this and set WorkItem.Status to "Suspended"
	// if older version is still ingesting.
	if b.StillIngestingOlderVersion(workItem) {
		return true
	}

	// There's a newer ingest request in Pharos' WorkItems list,
	// and we're not too far along to abandon this.
	if b.SupersededByNewerRequest(workItem) {
		b.PushToQueue(workItem, constants.IngestCleanup)
		return true
	}

	// In this case, there's a newer version of the bag in
	// the depositor's receiving bucket, and the Pharos WorkItems
	// list my not have even picked it up yet.
	//
	// The flag IngestObject.ShouldDeleteFromReceiving stays false
	// unless ingest is complete or the bag is in valid, so the
	// cleanup worker should not delete the newer item from receiving.
	if b.ShouldAbandonForNewerVersion(workItem) {
		b.PushToQueue(workItem, constants.IngestCleanup)
		return true
	}

	return false
}

// GetInstitutionIdentifier returns the identifier for the institution
// with the specified ID.
func (b *IngestBase) GetInstitutionIdentifier(instID int) (string, error) {
	if _, ok := b.institutionCache[instID]; !ok {
		v := url.Values{}
		v.Add("order", "name")
		v.Add("per_page", "200")
		resp := b.Context.PharosClient.InstitutionList(v)
		if resp.Error != nil {
			return "", resp.Error
		}
		for _, inst := range resp.Institutions() {
			b.institutionCache[inst.ID] = inst.Identifier
		}
	}
	return b.institutionCache[instID], nil
}

// GetIngestObject returns the IngestObject for the specified WorkItem from
// Redis, or it creates a new one. For the first phase of ingest (PreFetch),
// this will almost always have to create a new IngestObject. For subsequent
// phases, it should never have to create one.
func (b *IngestBase) GetIngestObject(workItem *registry.WorkItem) (*service.IngestObject, error) {
	ingestObject, err := b.Context.RedisClient.IngestObjectGet(workItem.ID, workItem.ObjectIdentifier)
	if err == nil && ingestObject != nil {
		return ingestObject, nil
	}
	if err != nil && b.NSQChannel != constants.IngestPreFetch {
		return nil, fmt.Errorf("Ingest object not found in Redis: %v", err)
	}
	instID, err := b.GetInstitutionIdentifier(workItem.InstitutionID)
	if err != nil {
		return nil, err
	}
	return service.NewIngestObject(
		workItem.Bucket,
		workItem.Name,
		workItem.ETag,
		instID,
		workItem.InstitutionID,
		workItem.Size,
	), nil
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
func (b *IngestBase) SaveWorkResult(workItemID int, result *service.WorkResult) error {
	for i := 0; i < 3; i++ {
		err := b.Context.RedisClient.WorkResultSave(workItemID, result)
		if err == nil {
			break
		}
		if i == 2 && err != nil {
			b.Context.Logger.Info("Error saving WorkResult for WorkItem %d: %v", workItemID, err)
			return err
		}
		time.Sleep(time.Duration(250) * time.Millisecond)
	}
	return nil
}

// SaveWorkItem saves a WorkItem back to Pharos.
func (b *IngestBase) SaveWorkItem(workItem *registry.WorkItem) error {
	resp := b.Context.PharosClient.WorkItemSave(workItem)
	if resp.Error != nil {
		b.Context.Logger.Error("Error saving WorkItem %d to Pharos: %v",
			workItem.ID, resp.Error)
		return resp.Error
	}
	return nil
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

// StillIngestingOlderVersion returns true if it looks like we're still
// processing an older ingest request for this bag.
func (b *IngestBase) StillIngestingOlderVersion(workItem *registry.WorkItem) bool {
	items := b.FindOtherIngestRequests(workItem)
	for _, item := range items {
		if item.Date.Before(workItem.Date) && item.Retry && !item.ProcessingHasCompleted() {
			message := fmt.Sprintf("Skipping WorkItem %d because a prior version of this bag is still being ingested in WorkItem %d.", workItem.ID, item.ID)
			b.Context.Logger.Info(message)
			workItem.MarkNoLongerInProgress(
				workItem.Stage,
				workItem.Status,
				message,
			)
			return true
		}
	}
	return false
}

// SupersededByNewerRequest returns true if Pharos has an ingest request
// for this same item that's newer than the one we're processing AND we're
// not already in a late stage of ingest.
func (b *IngestBase) SupersededByNewerRequest(workItem *registry.WorkItem) bool {
	newerWorkItem := b.FindNewerIngestRequest(workItem)
	if newerWorkItem != nil && !b.IsLateStageOfIngest() {
		message := fmt.Sprintf("Skipping WorkItem %d because a newer version of this bag is waiting to be ingested in WorkItem %d", workItem.ID, newerWorkItem.ID)
		b.Context.Logger.Info(message)
		workItem.MarkNoLongerInProgress(
			workItem.Stage,
			constants.StatusCancelled,
			message,
		)
		workItem.Retry = false
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

// IsLateStageOfIngest returns true if we're at or beyound a point in the
// ingest process where all of an object's files have been copied to the
// staging area. At this point most of the heavy work has been done, and
// the ingest workers no longer need to reference the object in the depositor's
// receiving bucket, so it's best to finish the ingest process, even if a newer
// ingest request is pending. If we have to complete this request and the
// newer one, the newer one will simply count as an update/reingest of the
// current object.
func (b *IngestBase) IsLateStageOfIngest() bool {
	return util.StringListContains(constants.LateStagesOfIngest, b.NSQTopic)
}

// ShouldAbandonForNewerVersion returns true and logs a message
// if the bag in the depositor's receiving bucket was altered
// after the WorkItem was created. In those cases, we typically want to
// stop ingesting the current bag, cancel the WorkItem, and get to work
// on the new bag. The exception is when we've reached the storage phase.
// At that point, we're committed. We should complete the current ingest
// and then process the new one as an update.
func (b *IngestBase) ShouldAbandonForNewerVersion(workItem *registry.WorkItem) bool {
	if !b.IsLateStageOfIngest() {
		objInfo, err := b.Context.S3StatObject(
			constants.StorageProviderAWS,
			workItem.Bucket,
			workItem.Name)
		if err != nil {
			if strings.Contains(err.Error(), "key does not exist") {
				message := fmt.Sprintf("Stopping work on WorkItem %d because bag %s was deleted from %s", workItem.ID, workItem.Name, workItem.Bucket)
				b.Context.Logger.Info(message)
				workItem.MarkNoLongerInProgress(
					workItem.Stage,
					constants.StatusCancelled,
					message,
				)
				workItem.Retry = false
				return true
			}
			// This should never happen, due to checks at startup that
			// panic if provider is missing.
			if strings.Contains(err.Error(), "No S3 client for provider") {
				message := fmt.Sprintf("Can't check S3 for %s/%s because there's no S3 provider for %s", workItem.Bucket, workItem.Name, constants.StorageProviderAWS)
				b.Context.Logger.Error(message)
				workItem.MarkNoLongerInProgress(
					workItem.Stage,
					workItem.Status,
					message,
				)
				return true
			}
		} else {
			// No error. We should have objInfo
			if objInfo.ETag != "" && strings.Replace(objInfo.ETag, "\"", "", -1) != workItem.ETag {
				message := fmt.Sprintf("Stopping work on WorkItem %d because a newer version of bag %s was found in %s", workItem.ID, workItem.Name, workItem.Bucket)
				b.Context.Logger.Info(message)
				workItem.MarkNoLongerInProgress(
					workItem.Stage,
					constants.StatusCancelled,
					message,
				)
				workItem.Retry = false
				return true
			}
		}
	}
	return false
}

// MarkAsStarted tells Pharos, Redis, and NSQ that work on this
// item has started.
func (b *IngestBase) MarkAsStarted(ingestItem *IngestItem) {
	// Redis...
	ingestItem.WorkResult.Reset()
	ingestItem.WorkResult.Attempt++
	ingestItem.WorkResult.Host, _ = os.Hostname()
	ingestItem.WorkResult.Pid = os.Getpid()
	b.SaveWorkResult(ingestItem.WorkItem.ID, ingestItem.WorkResult)

	// Pharos...
	ingestItem.WorkItem.MarkInProgress(
		ingestItem.WorkItem.Stage,
		constants.StatusStarted,
		fmt.Sprintf("Item has started stage %s", ingestItem.WorkItem.Stage),
	)
	b.SaveWorkItem(ingestItem.WorkItem)

	// NSQ. Note that this disables NSQ autoresponse, and pings
	// NSQ every few minutes to say we're still working on the item.
	ingestItem.NSQStart()
}

// PushToQueue pushes the specified WorkItem to the named nsqTopic.
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
	} else {
		b.Context.Logger.Info("Pushed WorkItem %d (%s/%s) to NSQ topic %s",
			workItem.ID, workItem.Bucket, workItem.Name, nsqTopic)
	}
}
