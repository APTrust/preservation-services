package workers

import (
	"fmt"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/deletion"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/nsqio/go-nsq"
)

// Deleter is a worker that processes file and object deletion requests.
type Deleter struct {
	Base
}

// NewDeleter creates a new Deleter worker. Param context is a
// Context object with connections to S3, Redis, Pharos, and NSQ.
func NewDeleter(context *common.Context, settings *Settings) *Deleter {
	deleter := &Deleter{
		Base: Base{
			Context:           context,
			ItemsInProcess:    service.NewRingList(settings.ChannelBufferSize),
			ProcessChannel:    make(chan *Task, settings.ChannelBufferSize),
			SuccessChannel:    make(chan *Task, settings.ChannelBufferSize),
			ErrorChannel:      make(chan *Task, settings.ChannelBufferSize),
			FatalErrorChannel: make(chan *Task, settings.ChannelBufferSize),
		},
	}

	context.Logger.Info("Delete worker started with the following settings:")
	context.Logger.Info(settings.ToJSON())
	context.Logger.Info("Config settings (omitting sensitive credentials):")
	context.Logger.Info(context.Config.ToJSON())

	// Spin up the go routines that will act as workers
	for i := 0; i < settings.NumberOfWorkers; i++ {
		go deleter.ProcessItem()
	}
	go deleter.ProcessErrorChannel()
	go deleter.ProcessFatalErrorChannel()
	go deleter.ProcessSuccessChannel()

	return deleter
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
	task, _ := d.GetTaskObject(message, workItem, workResult)

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

func (d *Deleter) GetTaskObject(message *nsq.Message, workItem *registry.WorkItem, workResult *service.WorkResult) (*Task, error) {
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
	return task, nil
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
