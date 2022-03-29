package workers

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/network"
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
	ShouldRetry(*registry.WorkItem) bool
	AddToInProcessList(int)
	RemoveFromInProcessList(int)
	MarkAsStarted(*Task)
	FinishItem(*Task)
	PushToQueue(*registry.WorkItem, string)
}

// SigTermState contains info about whether the current worker
// received SIGTERM (or SIGINT), and if so, what action it took
// in response to the signal.
type SigTermState struct {
	// Received indicates whether this worker received SIGTERM
	// or SIGINT.
	Received bool
	// Completed indicates whether this worker completed all of
	// its SIGTERM cleanup tasks.
	Completed bool
	// ItemsInProcess is the number of items this worker was
	// working on when SIGTERM was received.
	ItemsInProcess int
	// ItemsRelease is the number of WorkItems this worker released
	// in Registry by clearing the WorkItem's Node and PID settings.
	ItemsReleased int
	// FailedToRelease is the number of WorkItems this worker tried
	// unsuccessfully to release.
	FailedReleases int
}

// Base contains the fundamental structures common to all workers.
type Base struct {

	// Context contains info about the context in which the worker is
	// operation, including connections to NSQ, Redis, Registry, and S3.
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

	// KillChannel handles SIGTERM and SIGINT.
	KillChannel chan os.Signal

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
	institutionCache map[int64]string

	// NSQConsumer implements HandleMessage to receive messages from NSQ.
	NSQConsumer *nsq.Consumer

	// processorConstructor is a function that returns an instance of
	// *ingest.Base that will handle the processing for this worker.
	processorConstructor ingest.BaseConstructor

	// sigTermState contains info about whether the current worker received
	// SIGTERM or SIGINT, and what cleanup work it did after receiving the
	// signal.
	sigTermState SigTermState
}

// RegisterAsNsqConsumer registers this worker as an NSQ consumer on
// Settings.NSQTopic and Settings.NSQChannel. Note that as soon as you
// call this, your worker will start handling messages if any are
// available.
func (b *Base) RegisterAsNsqConsumer() error {
	config := nsq.NewConfig()
	//config.Set("msg_timeout", "600m")
	config.Set("heartbeat_interval", "10s")
	config.Set("max_in_flight", b.Settings.ChannelBufferSize)
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
	// Get the WorkItem from Registry. If we can't, it's fatal.
	workItem, procErr := b.GetWorkItem(message)
	if procErr != nil && procErr.IsFatal {
		b.Context.Logger.Error(procErr.Error())
		return fmt.Errorf(procErr.Error())
	}

	// If there's any reason to skip this, return nil to tell
	// NSQ it's done. We haven't yet marked this WorkItem as
	// started, so do not save it back to Registry if we're going
	// to skip it. Doing so is the likely cause of a race condition
	// that resulted in the sporadically stalled items recorded in
	// https://trello.com/c/AsPdzfLi
	if b.ShouldSkipThis(workItem) {
		b.Context.Logger.Infof("Skipping WorkItem %d (%s)", workItem.ID, workItem.Name)
		return nil
	}

	workResult := b.GetWorkResult(workItem.ID)
	task, err := b.GetTaskObject(message, workItem, workResult)
	if err != nil {
		b.Context.Logger.Errorf("Could not get Task for WorkItem %d (%s): %v", workItem.ID, workItem.Name, err)
		return err
	}

	// Tell Registry and Redis we're starting work on this
	b.MarkAsStarted(task)

	// Make a note that we're processing this.
	b.AddToInProcessList(workItem.ID)

	// Put the item into the PreProcess channel, which
	// will set up the Processor to handle it.
	b.ProcessChannel <- task

	// Return nil (no error) so NSQ knows we're working on this.
	return nil
}

// ProcessItem calls task.Processor.Run() and then routes the
// task to the SuccessChannel, the ErrorChannel, or the
// FatalErrorChannel, depending on the outcome.
func (b *Base) ProcessItem() {
	for {
		select {
		case signal := <-b.KillChannel:
			b.doSigTermCleanup(signal)
		case task := <-b.ProcessChannel:
			b.processItem(task)
		}
	}
}

func (b *Base) processItem(task *Task) {
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

// GetWorkItem returns the WorkItem we should be working on.
func (b *Base) GetWorkItem(message *nsq.Message) (*registry.WorkItem, *service.ProcessingError) {
	msgBody := strings.TrimSpace(string(message.Body))
	b.Context.Logger.Info("NSQ Message body: ", msgBody)
	workItemID, err := strconv.ParseInt(string(msgBody), 10, 64)
	if err != nil || workItemID == 0 {
		fullErr := fmt.Errorf("Could not get WorkItemId from NSQ message body: %v", err)
		return nil, b.Error(0, msgBody, fullErr, true)
	}
	resp := b.Context.RegistryClient.WorkItemByID(workItemID)
	if resp.Error != nil {
		fullErr := fmt.Errorf("Error getting WorkItem %d from Registry: %v", workItemID, resp.Error)
		return nil, b.Error(workItemID, msgBody, fullErr, true)
	}
	workItem := resp.WorkItem()
	if workItem == nil {
		fullErr := fmt.Errorf("Registry returned nil for WorkItem %d", workItemID)
		return nil, b.Error(workItemID, msgBody, fullErr, true)
	}
	b.Context.Logger.Info("Got WorkItem", workItem.ID)
	return workItem, nil
}

// Error creates a new ProcessingError.
func (b *Base) Error(workItemID int64, identifier string, err error, isFatal bool) *service.ProcessingError {
	return service.NewProcessingError(
		workItemID,
		identifier,
		err.Error(),
		isFatal,
	)
}

// GetInstitutionIdentifier returns the identifier for the institution
// with the specified ID.
func (b *Base) GetInstitutionIdentifier(instID int64) (string, error) {
	if _, ok := b.institutionCache[instID]; !ok {
		v := url.Values{}
		v.Add("sort", "name")
		v.Add("per_page", "200")
		resp := b.Context.RegistryClient.InstitutionList(v)
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
func (b *Base) GetWorkResult(workItemID int64) *service.WorkResult {
	workResult, err := b.Context.RedisClient.WorkResultGet(workItemID, b.Settings.NSQTopic)
	if err != nil {
		b.Context.Logger.Infof("No WorkResult in Redis for WorkItem %d. No problem. Creating a new one.", workItemID)
		workResult = service.NewWorkResult(b.Settings.NSQTopic)
	}
	return workResult
}

// SaveWorkResult saves a WorkResult to Redis and logs an error if any occurs.
// Will try three times, in case Redis is busy.
func (b *Base) SaveWorkResult(workItemID int64, result *service.WorkResult) error {
	// Don't save, because processing is done and we don't
	// want to leave orphan records in Redis.
	if b.Settings.NextQueueTopic == "" {
		b.Context.Logger.Infof("Not saving WorkResult for WorkItem %d: No next queue topic", workItemID)
		return nil
	}
	for i := 0; i < 3; i++ {
		err := b.Context.RedisClient.WorkResultSave(workItemID, result)
		if err == nil {
			resultJSON, _ := result.ToJSON()
			b.Context.Logger.Infof("Saved result for WorkItem %d: %s", workItemID, resultJSON)
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

// SaveWorkItem saves a WorkItem back to Registry.
func (b *Base) SaveWorkItem(workItem *registry.WorkItem) error {
	var resp *network.RegistryResponse
	for i := 0; i < 5; i++ {
		resp = b.Context.RegistryClient.WorkItemSave(workItem)
		if resp.Error == nil {
			break
		} else {
			// Main problem here is 502/Bad Gateway, which seems
			// to happen in particular in the reingest check worker,
			// where turnaround between calls to Registry is a fraction
			// of a second.
			b.Context.Logger.Errorf(
				"Error saving WorkItem %d to Registry "+
					"(attempt %d, will retry in 1 second): %v",
				workItem.ID, i+1, resp.Error)
			time.Sleep(1 * time.Second)
		}
	}
	if resp.Error != nil {
		b.Context.Logger.Errorf("Error saving WorkItem %d to Registry "+
			"after max attempts: %v",
			workItem.ID, resp.Error)
		return resp.Error
	} else {
		jsonData, _ := workItem.ToJSON()
		b.Context.Logger.Infof("Saved WorkItem to Registry: %s", jsonData)
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
		b.Context.Logger.Infof("Skipping WorkItem %d because it's being processed by host %s, pid %d and this worker is host %s, pid %d", workItem.ID, workItem.Node, workItem.Pid, hostname, os.Getpid())
		return true
	}
	return false
}

// ImAlreadyProcessingThis returns true and logs a message if this WorkItem
// is already being processed by this worker. This happens with large bags
// when NSQ thinks the item has timed out and tries to reassign it to a new
// worker.
func (b *Base) ImAlreadyProcessingThis(workItem *registry.WorkItem) bool {
	if b.ItemsInProcess.Contains(strconv.FormatInt(workItem.ID, 10)) {
		// Node and pid may be empty if this was manually requeued. Reset them.
		workItem.SetNodeAndPid()
		b.Context.Logger.Infof("Skipping WorkItem %d because this worker is already working on it host %s, pid %d", workItem.ID, workItem.Node, workItem.Pid)
		return true
	}
	return false
}

// ShouldRetry marks a WorkItem as no longer in progress and logs a
// message to that effect if the WorkItem's Retry flag is false. It returns
// the value of WorkItem.Retry.
func (b *Base) ShouldRetry(workItem *registry.WorkItem) bool {
	if !workItem.Retry {
		message := fmt.Sprintf("Rejecting WorkItem %d because retry = false", workItem.ID)
		workItem.MarkNoLongerInProgress(
			workItem.Stage,
			workItem.Status,
			message,
		)
		b.Context.Logger.Info(message)
	}
	return workItem.Retry
}

// AddToInProcessList adds workItemID to this worker's ItemsInProcess list.
func (b *Base) AddToInProcessList(workItemID int64) {
	b.ItemsInProcess.Add(strconv.FormatInt(workItemID, 10))
}

// RemoveFromInProcessList removes workItemID from this worker's
// ItemsInProcess list.
func (b *Base) RemoveFromInProcessList(workItemID int64) {
	b.ItemsInProcess.Del(strconv.FormatInt(workItemID, 10))
}

// MarkAsStarted tells Registry, Redis, and NSQ that work on this
// item has started.
func (b *Base) MarkAsStarted(task *Task) {
	// Redis...
	b.Context.Logger.Infof("Starting Redis WorkResult for WorkItem %d (%s)", task.WorkItem.ID, task.WorkItem.Name)
	task.WorkResult.Reset()
	task.WorkResult.Attempt++
	task.WorkResult.Start()
	task.WorkResult.Host, _ = os.Hostname()
	task.WorkResult.Pid = os.Getpid()
	b.SaveWorkResult(task.WorkItem.ID, task.WorkResult)

	// Registry...
	b.Context.Logger.Infof("Telling Registry we're starting WorkItem %d (%s)", task.WorkItem.ID, task.WorkItem.Name)
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

// FinishItem updates NSQ and Registry, finishes and saves the WorkResult,
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

// doSigTermCleanup handles SIGTERM and SIGINT. AWS's Elastic Scaling
// service issues SIGTERM before SIGKILL, so we have time to clean up.
// If we've set stopTimeout to two minutes, we have two minutes to wrap
// up loose ends. For more on stopTimeout, see:
// https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_definition_parameters.html#container_definition_timeout
//
// We don't worry about items in the SuccessChannel, ErrorChannel or
// FatalError channel, because those chanels just do housekeeping, updating
// Redis, NSQ and Registry. That takes less than second under normal load,
// and maybe 3 seconds under the heaviest loads. There's typically only one
// item in those channels at any given time, and maybe a max of four items
// in the worst case. Those channels can complete their work without
// intervetion in the two minutes between SIGTERM and SIGKILL.
//
// The ProcessChannel is another issue. It may be working on tasks that take
// several minutes or even several hours each. When we get SIGKILL, those tasks
// die before they complete. That's OK, because Redis has enough info for the
// next worker, in some other container, to resume the dead task.
//
// However, SIGKILL leaves us with the following problems that this function
// must remediate:
//
// 1. Our NSQ messages have very long timeouts, up to 12 hours, and for good
// reason. (Try calculating checksums or doing network copies on 1TB files.)
// We want nsqd to know immediately that this worker is no longer active, so
// it doesn't wait 12 hours to requeue tasks for other workers.
//
// 2. We have to update this worker's WorkItems in Registry, clearing out the
// Node and PID settings, so other workers can claim this item. So long as the
// item has a non-empty Node and PID, other workers will think someone owns it,
// and they won't touch it.
func (b *Base) doSigTermCleanup(signal os.Signal) {
	if signal != syscall.SIGINT && signal != syscall.SIGTERM {
		return
	}
	b.sigTermState.Received = true
	b.Context.Logger.Warning("Worker received SIGTERM. Starting graceful shutdown.")

	if b.NSQConsumer != nil {
		// Stop the consumer. nsqd will pick this up
		// and requeue whatever messages we were working on,
		// so that other workers can pick them up. See the
		// section titled "Heartbeats and Timeouts" at
		// https://nsq.io/overview/internals.html
		b.Context.Logger.Warning("SIGTERM step 1: Disconnect from NSQ")
		b.NSQConsumer.ChangeMaxInFlight(0)
		b.NSQConsumer.Stop()
		b.Context.Logger.Warning("Worker disconnected from nsqd due to SIGTERM.")
	} else {
		b.Context.Logger.Warning("SIGTERM step 1: No need to stop NSQ consumer because there isn't one.")
	}

	// Now we have another problem. Even if these items
	// are requeued, no other worker will pick them up
	// because the WorkItem record in Registry says this
	// worker node and pid is still actively working on
	// the item. We need to update the Registry WorkItem
	// record to indicate that this worker no longer owns
	// it.
	b.Context.Logger.Warning("SIGTERM step 2: Release WorkItems")
	itemsInProcess := b.ItemsInProcess.Items()
	b.sigTermState.ItemsInProcess = len(itemsInProcess)
	for _, strItemID := range itemsInProcess {
		itemID, err := strconv.ParseInt(strItemID, 10, 64)
		if err != nil {
			releaseErr := b.sigTermReleaseWorkItem(itemID)
			if releaseErr != nil {
				b.sigTermState.FailedReleases += 1
				b.Context.Logger.Errorf("Could not release WorkItem %d after SIGTERM: %v", itemID, releaseErr)
			} else {
				b.sigTermState.ItemsReleased += 1
				b.Context.Logger.Warningf("Released WorkItem %d due to SIGTERM", itemID)
			}
		}
	}
	b.sigTermState.Completed = true
	b.Context.Logger.Warning("SIGTERM: Done releasing WorkItems")
	b.Context.Logger.Warning("SIGTERM: Graceful shutdown steps complete. Waiting for SIGKILL.")
}

// sigTermReleaseWorkItem clears the Node and PID, and sets the status
// to Pending on the specified WorkItem. This is used only when our worker
// gets a SIGTERM.
func (b *Base) sigTermReleaseWorkItem(itemID int64) error {
	resp := b.Context.RegistryClient.WorkItemByID(itemID)
	if resp.Error != nil {
		return resp.Error
	}
	item := resp.WorkItem()
	if item.Node != "" {
		// We haven't claimed this item yet,
		// so there's no need to release it.
		return nil
	}
	hostname, _ := os.Hostname()
	item.Node = ""
	item.Pid = 0
	item.Status = constants.StatusPending
	if !strings.Contains(item.Note, hostname) {
		item.Note = fmt.Sprintf("%s - Waiting for new worker because container %s was killed", item.Note, hostname)
	}
	return b.SaveWorkItem(item)
}

// GetSigTermState returns this worker's SigTermState object, which
// contains info about whether this worker received SIGTERM or SIGINT
// and what action it took.
func (b *Base) GetSigTermState() SigTermState {
	return b.sigTermState
}
