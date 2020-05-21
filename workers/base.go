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
	"github.com/nsqio/go-nsq"
)

// ServiceWorker defines the primary interface for service workers.
// Actual workers will implement other methods in addition to these.
type ServiceWorker interface {
	RegisterAsNsqConsumer() error
	HandleMessage(*nsq.Message) error
	ProcessSuccessChannel()
	ProcessErrorChannel()
	ProcessFatalErrorChannel()
	GetWorkItem(*nsq.Message) (*registry.WorkItem, *service.ProcessingError)
	Error(int, string, error, bool) *service.ProcessingError
	GetInstitutionIdentifier(int) (string, error)
	GetWorkResult(int) *service.WorkResult
	SaveWorkResult(int, *service.WorkResult) error
	SaveWorkItem(*registry.WorkItem) error
	OtherWorkerIsHandlingThis(*registry.WorkItem) bool
	ImAlreadyProcessingThis(*registry.WorkItem) bool
	AddToInProcessList(int)
	RemoveFromInProcessList(int)
	MarkAsStarted(*Task)
	FinishItem(*Task)
	PushToQueue(*registry.WorkItem, string)
}

// Base contains the fundamental structures common to all workers.
type Base struct {

	// Context contains info about the context in which the worker is
	// operation, including connections to NSQ, Redis, Pharos, and S3.
	Context *common.Context

	// ItemsInProcess keeps track of WorkItem ids that the worker is
	// currently processing. We need to do this because NSQ does not
	// dedupe messages, so the worker must.
	ItemsInProcess *service.RingList

	// ProcessChannel is where the work actually happens: validation,
	// storage, recording, etc., depending on the worker's responsibility.
	ProcessChannel chan *Task

	// SuccessChannel processes items that have gone through the
	// ProcessChannel with no errors.
	SuccessChannel chan *Task

	// ErrorChannel processes items that have gone through the
	// ProcessChannel with one or more non-fatal errors. These items
	// typically should be retried.
	ErrorChannel chan *Task

	// FatalErrorChannel processes items that have gone through the
	// ProcessChannel with one or more fatal errors. These items
	// typically should not be retried.
	FatalErrorChannel chan *Task

	// Settings contains information on what to do in post-processing
	// in the SuccessChannel, ErrorChannel, and FatalErrorChannel.
	Settings *Settings

	// ShouldSkipThis checks to see whether the worker should
	// skip this WorkItem. This is not implemented in Base itself.
	// It MUST be implemented in structs that derive from Base.
	ShouldSkipThis func(*registry.WorkItem) bool

	// GetTaskObject returns a Task object to be worked on.
	// This is not implemented in Base itself. It MUST be implemented
	// in structs that derive from Base.
	GetTaskObject func(*nsq.Message, *registry.WorkItem, *service.WorkResult) (*Task, error)

	// institutionCache maps institution ids to identifiers. The institution
	// identifier is typically a domain name like "virginia.edu", "test.org",
	// etc.
	institutionCache map[int]string

	// NSQConsumer implements HandleMessage to receive messages from NSQ.
	NSQConsumer *nsq.Consumer

	// processorConstructor is a function that returns an instance of
	// *ingest.Base that will handle the processing for this worker.
	processorConstructor ingest.BaseConstructor
}

// RegisterAsNsqConsumer registers this worker as an NSQ consumer on
// Settings.NSQTopic and Settings.NSQChannel. Note that as soon as you
// call this, your worker will start handling messages if any are
// available.
func (b *Base) RegisterAsNsqConsumer() error {
	config := nsq.NewConfig()
	//config.Set("msg_timeout", "600m")
	config.Set("heartbeat_interval", "10s")
	consumer, err := nsq.NewConsumer(b.Settings.NSQTopic, b.Settings.NSQChannel, config)
	if err != nil {
		return err
	}
	b.NSQConsumer = consumer
	b.NSQConsumer.AddHandler(b)
	b.NSQConsumer.ConnectToNSQLookupd(b.Context.Config.NsqLookupd)
	b.Context.Logger.Info("Registered as NSQ consumer")
	return nil
}

// HandleMessage checks to see whether we should process this message at
// all. If so, it packages up an IngestItem with everything except the
// Processor object (an instance of ingest.Base). It puts the IngestItem
// in the the PreProcessChannel. From there, the worker should instantiate
// and assign the right IngestItem.Processor type and push the item into
// the ProcessChannel.
func (b *Base) HandleMessage(message *nsq.Message) error {
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
		b.Context.Logger.Infof("Skipping WorkItem %d (%s)", workItem.ID, workItem.Name)
		return nil
	}

	workResult := b.GetWorkResult(workItem.ID)
	task, err := b.GetTaskObject(message, workItem, workResult)
	if err != nil {
		b.Context.Logger.Errorf("Could not get Task for WorkItem %d (%s): %v", workItem.ID, workItem.Name, err)
		return err
	}

	// Tell Pharos and Redis we're starting work on this
	b.MarkAsStarted(task)

	// Make a note that we're processing this.
	b.AddToInProcessList(workItem.ID)

	// Put the item into the PreProcess channel, which
	// will set up the Processor to handle it.
	b.ProcessChannel <- task

	// Return nil (no error) so NSQ knows we're working on this.
	return nil
}

// GetTaskObject returns an object representing the task to be implemented.
// This object will be passed from channel to channel during processing.
// func (b *Base) GetTaskObject(message *nsq.Message, workItem *registry.WorkItem, workResult *service.WorkResult) (*Task, error) {
// 	return nil, fmt.Errorf("GetTaskObject must be implemented in derived class")
// }

// ShouldSkipThis returns true if the worker should skip this item.
// func (b *Base) ShouldSkipThis(workItem *registry.WorkItem) bool {
// 	// Each base class has to implement this on its own
// 	return true
// }

// ProcessItem calls task.Processor.Run() and then routes the
// task to the SuccessChannel, the ErrorChannel, or the
// FatalErrorChannel, depending on the outcome.
func (b *Base) ProcessItem() {
	for task := range b.ProcessChannel {
		b.Context.Logger.Infof("WorkItem %d (%s) is in ProcessChannel", task.WorkItem.ID, task.WorkItem.Name)
		count, errors := task.Processor.Run()
		task.WorkResult.Errors = errors

		b.Context.Logger.Infof("WorkItem %d: count %d", task.WorkItem.ID, count)

		if task.WorkResult.HasFatalErrors() {
			b.FatalErrorChannel <- task
		} else if task.WorkResult.HasErrors() {
			b.ErrorChannel <- task
		} else {
			b.SuccessChannel <- task
		}
	}
}

func (b *Base) ProcessSuccessChannel() {
	for task := range b.SuccessChannel {
		b.Context.Logger.Infof("WorkItem %d (%s) is in success channel",
			task.WorkItem.ID, task.WorkItem.Name)
		// Tell Pharos item succeeded.
		task.WorkItem.Note = b.Settings.WorkItemSuccessNote
		task.WorkItem.Stage = b.Settings.NextWorkItemStage
		task.WorkItem.Status = constants.StatusPending
		task.WorkItem.Retry = true
		task.WorkItem.NeedsAdminReview = false

		// When cleaup succeeds, we need to mark the item as succeeded.
		if b.Settings.NSQTopic == constants.IngestCleanup {
			task.WorkItem.Status = constants.StatusSuccess
			task.WorkItem.Outcome = "Ingest complete"
			task.WorkItem.ObjectIdentifier = task.Processor.GetIngestObject().Identifier()
		}

		// Push item to next queue.
		task.NextQueueTopic = b.Settings.NextQueueTopic
		b.FinishItem(task)

		// Tell NSQ this b is done with this message.
		task.NSQFinish()
	}
}

func (b *Base) ProcessErrorChannel() {
	for task := range b.ErrorChannel {
		shouldRequeue := true
		b.Context.Logger.Warningf("WorkItem %d (%s) is in error channel",
			task.WorkItem.ID, task.WorkItem.Name)
		b.Context.Logger.Warningf("Non-fatal errors for WorkItem %d (%s): %s",
			task.WorkItem.ID, task.WorkItem.Name,
			task.WorkResult.NonFatalErrorMessage())

		// Update WorkItem in Pharos
		task.WorkItem.Note = task.WorkResult.NonFatalErrorMessage()
		if task.WorkResult.Attempt >= b.Settings.MaxAttempts {
			task.WorkItem.Note += fmt.Sprintf(" Will not retry: failed %d times. Interim processing data persists.", task.WorkResult.Attempt)
			task.WorkItem.Retry = false
			task.WorkItem.NeedsAdminReview = true
			shouldRequeue = false

			// Go to NSQ cleanup or not?
			if b.Settings.PushToCleanupAfterMaxFailedAttempts {
				task.Processor.GetIngestObject().ShouldDeleteFromReceiving = b.Settings.DeleteFromReceivingAfterMaxFailedAttempts
				task.NextQueueTopic = constants.IngestCleanup
			} else {
				task.NextQueueTopic = ""
			}
		} else {
			// Processing failed due to non-fatal (transient) errors,
			// and we haven't reached MaxAttempts. Don't push to next
			// queue. We'll requeue below.
			task.NextQueueTopic = ""
		}

		b.FinishItem(task)
		if shouldRequeue {
			task.NSQRequeue(b.Settings.RequeueTimeout)
		} else {
			task.NSQFinish()
		}
	}
}

func (b *Base) ProcessFatalErrorChannel() {
	for task := range b.FatalErrorChannel {
		b.Context.Logger.Errorf("WorkItem %d (%s) is in fatal error channel",
			task.WorkItem.ID, task.WorkItem.Name)
		b.Context.Logger.Errorf("Fatal errors for WorkItem %d (%s): %s",
			task.WorkItem.ID, task.WorkItem.Name,
			task.WorkResult.FatalErrorMessage())

		// Update WorkItem for Pharos
		task.WorkItem.Note = task.WorkResult.FatalErrorMessage()
		task.WorkItem.Retry = false
		task.WorkItem.NeedsAdminReview = true

		// NSQ
		if b.Settings.PushToCleanupOnFatalError {
			task.Processor.GetIngestObject().ShouldDeleteFromReceiving = b.Settings.DeleteFromReceivingAfterFatalError
			task.NextQueueTopic = constants.IngestCleanup
		} else {
			task.NextQueueTopic = ""
		}

		// Update Pharos and Redis, and send to next queue if required.
		b.FinishItem(task)

		// Tell NSQ we're done with this message.
		task.NSQFinish()
	}
}

// GetWorkItem returns the WorkItem we should be working on.
func (b *Base) GetWorkItem(message *nsq.Message) (*registry.WorkItem, *service.ProcessingError) {
	msgBody := strings.TrimSpace(string(message.Body))
	b.Context.Logger.Info("NSQ Message body: ", msgBody)
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
	b.Context.Logger.Info("Got WorkItem", workItem.ID)
	return workItem, nil
}

// Error creates a new ProcessingError.
func (b *Base) Error(workItemID int, identifier string, err error, isFatal bool) *service.ProcessingError {
	return service.NewProcessingError(
		workItemID,
		identifier,
		err.Error(),
		isFatal,
	)
}

// GetInstitutionIdentifier returns the identifier for the institution
// with the specified ID.
func (b *Base) GetInstitutionIdentifier(instID int) (string, error) {
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

// GetWorkResult returns an WorkResult object for this WorkItem. If one
// already exists in Redis, it returns that. If not, it creates a new one.
func (b *Base) GetWorkResult(workItemID int) *service.WorkResult {
	workResult, err := b.Context.RedisClient.WorkResultGet(workItemID, b.Settings.NSQTopic)
	if err != nil {
		b.Context.Logger.Infof("No WorkResult in Redis for WorkItem %d. No problem. Creating a new one.", workItemID)
		workResult = service.NewWorkResult(b.Settings.NSQTopic)
	}
	return workResult
}

// SaveWorkResult saves a WorkResult to Redis and logs an error if any occurs.
// Will try three times, in case Redis is busy.
func (b *Base) SaveWorkResult(workItemID int, result *service.WorkResult) error {
	for i := 0; i < 3; i++ {
		err := b.Context.RedisClient.WorkResultSave(workItemID, result)
		if err == nil {
			break
		}
		if i == 2 && err != nil {
			b.Context.Logger.Infof("Error saving WorkResult for WorkItem %d: %v", workItemID, err)
			return err
		}
		time.Sleep(time.Duration(250) * time.Millisecond)
	}
	return nil
}

// SaveWorkItem saves a WorkItem back to Pharos.
func (b *Base) SaveWorkItem(workItem *registry.WorkItem) error {
	resp := b.Context.PharosClient.WorkItemSave(workItem)
	if resp.Error != nil {
		b.Context.Logger.Error("Error saving WorkItem %d to Pharos: %v",
			workItem.ID, resp.Error)
		return resp.Error
	}
	return nil
}

// OtherWorkerIsHandlingThis returns true if some other worker is already
// processing this message. This happens often with large ingests that
// take longer to process than NSQ's maximum allowed timeout.
func (b *Base) OtherWorkerIsHandlingThis(workItem *registry.WorkItem) bool {
	if workItem.Node == "" && workItem.Pid == 0 {
		return false
	}
	hostname, _ := os.Hostname()
	if workItem.Node != hostname || workItem.Pid != os.Getpid() {
		b.Context.Logger.Infof("Skipping WorkItem %d because it's being processed by host %s, pid %d", workItem.ID, workItem.Node, workItem.Pid)
		return true
	}
	return false
}

// ImAlreadyProcessingThis returns true and logs a message if this WorkItem
// is already being processed by this worker. This happens with large bags
// when NSQ thinks the item has timed out and tries to reassign it to a new
// worker.
func (b *Base) ImAlreadyProcessingThis(workItem *registry.WorkItem) bool {
	if b.ItemsInProcess.Contains(strconv.Itoa(workItem.ID)) {
		b.Context.Logger.Infof("Skipping WorkItem %d because this worker is already working on it host %s, pid %d", workItem.ID, workItem.Node, workItem.Pid)
		return true
	}
	return false
}

// AddToInProcessList adds workItemID to this worker's ItemsInProcess list.
func (b *Base) AddToInProcessList(workItemID int) {
	b.ItemsInProcess.Add(strconv.Itoa(workItemID))
}

// RemoveFromInProcessList removes workItemID from this worker's
// ItemsInProcess list.
func (b *Base) RemoveFromInProcessList(workItemID int) {
	b.ItemsInProcess.Del(strconv.Itoa(workItemID))
}

// MarkAsStarted tells Pharos, Redis, and NSQ that work on this
// item has started.
func (b *Base) MarkAsStarted(task *Task) {
	// Redis...
	b.Context.Logger.Infof("Starting Redis WorkResult for WorkItem %d (%s)", task.WorkItem.ID, task.WorkItem.Name)
	task.WorkResult.Reset()
	task.WorkResult.Attempt++
	task.WorkResult.Host, _ = os.Hostname()
	task.WorkResult.Pid = os.Getpid()
	b.SaveWorkResult(task.WorkItem.ID, task.WorkResult)

	// Pharos...
	b.Context.Logger.Infof("Telling Pharos we're starting WorkItem %d (%s)", task.WorkItem.ID, task.WorkItem.Name)
	task.WorkItem.MarkInProgress(
		task.WorkItem.Stage,
		constants.StatusStarted,
		fmt.Sprintf("Item has started stage %s", b.Settings.NSQTopic),
	)
	b.SaveWorkItem(task.WorkItem)

	// NSQ. Note that this disables NSQ autoresponse, and pings
	// NSQ every few minutes to say we're still working on the item.
	b.Context.Logger.Infof("Telling NSQ we're starting WorkItem %d (%s)", task.WorkItem.ID, task.WorkItem.Name)
	task.NSQStart()
}

// FinishItem updates NSQ and Pharos, finishes and saves the WorkResult,
// and removes this item from the ItemsInProcess list.
func (b *Base) FinishItem(task *Task) {
	b.Context.Logger.Infof("Finishing WorkItem %d (%s)", task.WorkItem.ID, task.WorkItem.Name)
	task.WorkItem.Node = ""
	task.WorkItem.Pid = 0
	b.SaveWorkItem(task.WorkItem)
	task.WorkResult.Finish()
	b.SaveWorkResult(task.WorkItem.ID, task.WorkResult)
	if task.NextQueueTopic != "" {
		b.PushToQueue(task.WorkItem, task.NextQueueTopic)
	}
	b.RemoveFromInProcessList(task.WorkItem.ID)
}

// PushToQueue pushes the specified WorkItem to the named nsqTopic.
func (b *Base) PushToQueue(workItem *registry.WorkItem, nsqTopic string) {
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
		b.Context.Logger.Infof("Pushed WorkItem %d (%s/%s) to NSQ topic %s",
			workItem.ID, workItem.Bucket, workItem.Name, nsqTopic)
	}
}
