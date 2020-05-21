package workers

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/deletion"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/nsqio/go-nsq"
)

// *********************************************************************
// TODO: Factor out methods common to this and IngestBase.
// *********************************************************************

// Deleter is a worker that processes file and object deletion requests.
type Deleter struct {

	// Context contains info about the context in which the worker is
	// operation, including connections to NSQ, Redis, Pharos, and S3.
	Context *common.Context

	// ItemsInProcess keeps track of WorkItem ids that the worker is
	// currently processing. We need to do this because NSQ does not
	// dedupe messages, so the worker must.
	ItemsInProcess *service.RingList

	// ProcessChannel is where the work actually happens.
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

	// NSQConsumer implements HandleMessage to receive messages from NSQ.
	NSQConsumer *nsq.Consumer

	// Settings contains info about number of workers, max attempts, etc.
	Settings *DeleteWorkerSettings
}

// NewDeleter creates a new Deleter worker. Param context is a
// Context object with connections to S3, Redis, Pharos, and NSQ.
func NewDeleter(context *common.Context, settings *DeleteWorkerSettings) *Deleter {
	base := &Deleter{
		Context:           context,
		ItemsInProcess:    service.NewRingList(settings.ChannelBufferSize),
		ProcessChannel:    make(chan *Task, settings.ChannelBufferSize),
		SuccessChannel:    make(chan *Task, settings.ChannelBufferSize),
		ErrorChannel:      make(chan *Task, settings.ChannelBufferSize),
		FatalErrorChannel: make(chan *Task, settings.ChannelBufferSize),
	}

	context.Logger.Info("Delete worker started with the following settings:")
	context.Logger.Info(settings.ToJSON())
	context.Logger.Info("Config settings (omitting sensitive credentials):")
	context.Logger.Info(context.Config.ToJSON())

	// Spin up the go routines that will act as workers
	for i := 0; i < settings.NumberOfWorkers; i++ {
		go base.processItem()
	}
	go base.ProcessErrorChannel()
	go base.ProcessFatalErrorChannel()
	go base.ProcessSuccessChannel()

	return base
}

// RegisterAsNsqConsumer registers this worker as an NSQ consumer on
// Settings.NSQTopic and Settings.NSQChannel. Note that as soon as you
// call this, your worker will start handling messages if any are
// available.
func (d *Deleter) RegisterAsNsqConsumer() error {
	config := nsq.NewConfig()
	//config.Set("msg_timeout", "600m")
	config.Set("heartbeat_interval", "10s")
	consumer, err := nsq.NewConsumer(d.Settings.NSQTopic, d.Settings.NSQChannel, config)
	if err != nil {
		return err
	}
	d.NSQConsumer = consumer
	d.NSQConsumer.AddHandler(d)
	d.NSQConsumer.ConnectToNSQLookupd(d.Context.Config.NsqLookupd)
	return nil
}

// HandleMessage checks to see whether we should process this message at
// all. If so, it packages up an IngestItem with everything except the
// Processor object (an instance of ingest.Base). It puts the IngestItem
// in the the PreProcessChannel. From there, the worker should instantiate
// and assign the right IngestItem.Processor type and push the item into
// the ProcessChannel.
func (d *Deleter) HandleMessage(message *nsq.Message) error {
	// Get the WorkItem from Pharos. If we can't, it's fatal.
	workItem, procErr := d.GetWorkItem(message)
	if procErr != nil && procErr.IsFatal {
		d.Context.Logger.Error(procErr.Error())
		return fmt.Errorf(procErr.Error())
	}

	// If there's any reason to skip this, return nil to tell
	// NSQ it's done. This function also sets some properties on
	// the WorkItem, so admins and depositors will know the item's
	// state. So save the WorkItem before returning.
	if d.ShouldSkipThis(workItem) {
		d.SaveWorkItem(workItem)
		return nil
	}

	workResult := d.GetWorkResult(workItem.ID)

	// Set up the deletion manager, which actually deletes
	// the files.
	identifier := workItem.GenericFileIdentifier
	itemType := constants.TypeFile
	if identifier == "" && workItem.ObjectIdentifier != "" {
		identifier = workItem.ObjectIdentifier
		itemType = constants.TypeObject
	}
	deletionManager := deletion.NewManager(
		d.Context,
		workItem.ID,
		identifier,
		itemType,
		workItem.User, // requested by
		workItem.InstApprover,
		workItem.APTrustApprover,
	)

	// Set up the deletion item, which is packages all the info
	// that needs to be passed from channel to channel.
	task := &Task{
		Processor:  deletionManager,
		NSQMessage: message,
		WorkItem:   workItem,
		WorkResult: workResult,
	}

	// Tell Pharos and Redis we're starting work on this
	d.MarkAsStarted(task)

	// Make a note that we're processing this.
	d.AddToInProcessList(workItem.ID)

	// Put the item into the PreProcess channel, which
	// will set up the Processor to handle it.
	d.ProcessChannel <- task

	// Return nil (no error) so NSQ knows we're working on this.
	return nil
}

// processItem deletes files from storage and then routes the
// task to the SuccessChannel, the ErrorChannel, or the
// FatalErrorChannel, depending on the outcome.
func (d *Deleter) processItem() {
	for task := range d.ProcessChannel {
		count, errors := task.Processor.Run()
		task.WorkResult.Errors = errors

		d.Context.Logger.Infof("WorkItem %d: deleted count %d files", task.WorkItem.ID, count)

		if task.WorkResult.HasFatalErrors() {
			d.FatalErrorChannel <- task
		} else if task.WorkResult.HasErrors() {
			d.ErrorChannel <- task
		} else {
			d.SuccessChannel <- task
		}
	}
}

func (d *Deleter) ProcessSuccessChannel() {
	for task := range d.SuccessChannel {
		d.Context.Logger.Infof("WorkItem %d (%s) is in success channel",
			task.WorkItem.ID, task.WorkItem.Name)
		// Tell Pharos item succeeded.
		task.WorkItem.Note = d.Settings.WorkItemSuccessNote
		task.WorkItem.Stage = constants.StageResolve
		task.WorkItem.Status = constants.StatusSuccess
		task.WorkItem.Retry = true
		task.WorkItem.NeedsAdminReview = false

		// Tell NSQ this b is done with this message.
		task.NSQFinish()
	}
}

func (d *Deleter) ProcessErrorChannel() {
	for task := range d.ErrorChannel {
		shouldRequeue := true
		d.Context.Logger.Warningf("WorkItem %d (%s) is in error channel",
			task.WorkItem.ID, task.WorkItem.Name)
		d.Context.Logger.Warningf("Non-fatal errors for WorkItem %d (%s): %s",
			task.WorkItem.ID, task.WorkItem.Name,
			task.WorkResult.NonFatalErrorMessage())

		// Update WorkItem in Pharos
		task.WorkItem.Note = task.WorkResult.NonFatalErrorMessage()
		if task.WorkResult.Attempt >= d.Settings.MaxAttempts {
			task.WorkItem.Note += fmt.Sprintf(" Will not retry: failed %d times. Interim processing data persists.", task.WorkResult.Attempt)
			task.WorkItem.Retry = false
			task.WorkItem.NeedsAdminReview = true
			shouldRequeue = false
		}
		d.FinishItem(task)
		if shouldRequeue {
			task.NSQRequeue(d.Settings.RequeueTimeout)
		} else {
			task.NSQFinish()
		}
	}
}

func (d *Deleter) ProcessFatalErrorChannel() {
	for task := range d.FatalErrorChannel {
		d.Context.Logger.Errorf("WorkItem %d (%s) is in fatal error channel",
			task.WorkItem.ID, task.WorkItem.Name)
		d.Context.Logger.Errorf("Fatal errors for WorkItem %d (%s): %s",
			task.WorkItem.ID, task.WorkItem.Name,
			task.WorkResult.FatalErrorMessage())

		// Update WorkItem for Pharos
		task.WorkItem.Note = task.WorkResult.FatalErrorMessage()
		task.WorkItem.Retry = false
		task.WorkItem.NeedsAdminReview = true

		// Update Pharos and Redis.
		d.FinishItem(task)

		// Tell NSQ we're done with this message.
		task.NSQFinish()
	}
}

// GetWorkItem returns the WorkItem we should be working on.
func (d *Deleter) GetWorkItem(message *nsq.Message) (*registry.WorkItem, *service.ProcessingError) {
	msgBody := strings.TrimSpace(string(message.Body))
	d.Context.Logger.Info("NSQ Message body: ", msgBody)
	workItemID, err := strconv.Atoi(string(msgBody))
	if err != nil || workItemID == 0 {
		fullErr := fmt.Errorf("Could not get WorkItemId from NSQ message body: %v", err)
		return nil, d.Error(0, msgBody, fullErr, false)
	}
	resp := d.Context.PharosClient.WorkItemGet(workItemID)
	if resp.Error != nil {
		fullErr := fmt.Errorf("Error getting WorkItem %d from Pharos: %v", workItemID, resp.Error)
		return nil, d.Error(workItemID, msgBody, fullErr, false)
	}
	workItem := resp.WorkItem()
	if workItem == nil {
		fullErr := fmt.Errorf("Pharos returned nil for WorkItem %d", workItemID)
		return nil, d.Error(workItemID, msgBody, fullErr, true)
	}
	return workItem, nil
}

// Error creates a new ProcessingError.
func (d *Deleter) Error(workItemID int, identifier string, err error, isFatal bool) *service.ProcessingError {
	return service.NewProcessingError(
		workItemID,
		identifier,
		err.Error(),
		isFatal,
	)
}

// ShouldSkipThis returns true if there are any reasons not process this
// WorkItem.
func (d *Deleter) ShouldSkipThis(workItem *registry.WorkItem) bool {

	// It's possible that another worker recently marked this as
	// "do not retry." If that's the case, skip it.
	if workItem.Retry == false {
		message := fmt.Sprintf("Rejecting WorkItem %d because retry = false", workItem.ID)
		workItem.MarkNoLongerInProgress(
			workItem.Stage,
			workItem.Status,
			message,
		)
		d.Context.Logger.Info(message)
		return true
	}

	// Definitely don't delete this if it's not a deletion request.
	if workItem.Action != constants.ActionDelete {
		message := fmt.Sprintf("Rejecting WorkItem %d because action is %s, not 'Delete'", workItem.ID, workItem.Action)
		workItem.Retry = false
		workItem.MarkNoLongerInProgress(
			workItem.Stage,
			constants.StatusCancelled,
			message,
		)
		d.Context.Logger.Info(message)
		return true
	}

	// Do not proceed without the approval of institutional admin.
	if workItem.InstApprover == "" {
		message := fmt.Sprintf("Rejecting WorkItem %d because institutional approver is missing", workItem.ID)
		workItem.Retry = false
		workItem.MarkNoLongerInProgress(
			workItem.Stage,
			constants.StatusCancelled,
			message,
		)
		d.Context.Logger.Info(message)
		return true
	}

	// Occasionally, NSQ will think an item has timed out because
	// it took a long time to record. NSQ sends it to a new worker
	// after the original worker has completed it.
	if workItem.ProcessingHasCompleted() {
		message := fmt.Sprintf("Rejecting WorkItem %d because status is %s", workItem.ID, workItem.Status)
		d.Context.Logger.Info(message)
		return true
	}

	// Note that returning nil tells NSQ that a worker is
	// working on this item, even if it's not us. We don't
	// want to requeue duplicates, and we don't want to return
	// an error, because that's equivalent to FIN/failed.
	if d.OtherWorkerIsHandlingThis(workItem) {
		return true
	}

	// See if this worker is already processing this item.
	// This happens sometimes when NSQ thinks the item has
	// timed out while a worker is validating or storing
	// an object.
	if d.ImAlreadyProcessingThis(workItem) {
		return true
	}

	return false
}

// GetWorkResult returns an WorkResult object for this WorkItem. If one
// already exists in Redis, it returns that. If not, it creates a new one.
func (d *Deleter) GetWorkResult(workItemID int) *service.WorkResult {
	workResult, err := d.Context.RedisClient.WorkResultGet(workItemID, d.Settings.NSQTopic)
	if err != nil {
		d.Context.Logger.Infof("No WorkResult in Redis for WorkItem %d. No problem. Creating a new one.", workItemID)
		workResult = service.NewWorkResult(d.Settings.NSQTopic)
	}
	return workResult
}

// SaveWorkResult saves a WorkResult to Redis and logs an error if any occurs.
// Will try three times, in case Redis is busy.
func (d *Deleter) SaveWorkResult(workItemID int, result *service.WorkResult) error {
	for i := 0; i < 3; i++ {
		err := d.Context.RedisClient.WorkResultSave(workItemID, result)
		if err == nil {
			break
		}
		if i == 2 && err != nil {
			d.Context.Logger.Infof("Error saving WorkResult for WorkItem %d: %v", workItemID, err)
			return err
		}
		time.Sleep(time.Duration(250) * time.Millisecond)
	}
	return nil
}

// SaveWorkItem saves a WorkItem back to Pharos.
func (d *Deleter) SaveWorkItem(workItem *registry.WorkItem) error {
	resp := d.Context.PharosClient.WorkItemSave(workItem)
	if resp.Error != nil {
		d.Context.Logger.Error("Error saving WorkItem %d to Pharos: %v",
			workItem.ID, resp.Error)
		return resp.Error
	}
	return nil
}

// OtherWorkerIsHandlingThis returns true if some other worker is already
// processing this message. This happens often with large ingests that
// take longer to process than NSQ's maximum allowed timeout.
func (d *Deleter) OtherWorkerIsHandlingThis(workItem *registry.WorkItem) bool {
	if workItem.Node == "" && workItem.Pid == 0 {
		return false
	}
	hostname, _ := os.Hostname()
	if workItem.Node != hostname || workItem.Pid != os.Getpid() {
		d.Context.Logger.Infof("Skipping WorkItem %d because it's being processed by host %s, pid %d", workItem.ID, workItem.Node, workItem.Pid)
		return true
	}
	return false
}

// ImAlreadyProcessingThis returns true and logs a message if this WorkItem
// is already being processed by this worker. This happens with large bags
// when NSQ thinks the item has timed out and tries to reassign it to a new
// worker.
func (d *Deleter) ImAlreadyProcessingThis(workItem *registry.WorkItem) bool {
	if d.ItemsInProcess.Contains(strconv.Itoa(workItem.ID)) {
		d.Context.Logger.Infof("Skipping WorkItem %d because this worker is already working on it host %s, pid %d", workItem.ID, workItem.Node, workItem.Pid)
		return true
	}
	return false
}

// AddToInProcessList adds workItemID to this worker's ItemsInProcess list.
func (d *Deleter) AddToInProcessList(workItemID int) {
	d.ItemsInProcess.Add(strconv.Itoa(workItemID))
}

// RemoveFromInProcessList removes workItemID from this worker's
// ItemsInProcess list.
func (d *Deleter) RemoveFromInProcessList(workItemID int) {
	d.ItemsInProcess.Del(strconv.Itoa(workItemID))
}

// MarkAsStarted tells Pharos, Redis, and NSQ that work on this
// item has started.
func (d *Deleter) MarkAsStarted(task *Task) {
	// Redis...
	task.WorkResult.Reset()
	task.WorkResult.Attempt++
	task.WorkResult.Host, _ = os.Hostname()
	task.WorkResult.Pid = os.Getpid()
	d.SaveWorkResult(task.WorkItem.ID, task.WorkResult)

	// Pharos...
	task.WorkItem.MarkInProgress(
		task.WorkItem.Stage,
		constants.StatusStarted,
		fmt.Sprintf("Item has started stage %s", d.Settings.NSQTopic),
	)
	d.SaveWorkItem(task.WorkItem)

	// NSQ. Note that this disables NSQ autoresponse, and pings
	// NSQ every few minutes to say we're still working on the item.
	task.NSQStart()
}

// FinishItem updates NSQ and Pharos, finishes and saves the WorkResult,
// and removes this item from the ItemsInProcess list.
func (d *Deleter) FinishItem(task *Task) {
	task.WorkItem.Node = ""
	task.WorkItem.Pid = 0
	d.SaveWorkItem(task.WorkItem)
	task.WorkResult.Finish()
	d.SaveWorkResult(task.WorkItem.ID, task.WorkResult)
	d.RemoveFromInProcessList(task.WorkItem.ID)
}
