package workers

import (
	"fmt"
	"time"

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
func NewDeleter(bufSize, numWorkers, maxAttempts int) *Deleter {
	settings := &Settings{
		ChannelBufferSize: bufSize,
		MaxAttempts:       maxAttempts,
		NSQChannel:        constants.Deleter + "_worker_chan",
		NSQTopic:          constants.Deleter,
		NextQueueTopic:    "",
		NextWorkItemStage: constants.StageResolve,
		NumberOfWorkers:   numWorkers,
		RequeueTimeout:    (1 * time.Minute),
	}
	deleter := &Deleter{
		Base: Base{
			Context:           common.NewContext(),
			Settings:          settings,
			ItemsInProcess:    service.NewRingList(settings.ChannelBufferSize),
			ProcessChannel:    make(chan *Task, settings.ChannelBufferSize),
			SuccessChannel:    make(chan *Task, settings.ChannelBufferSize),
			ErrorChannel:      make(chan *Task, settings.ChannelBufferSize),
			FatalErrorChannel: make(chan *Task, settings.ChannelBufferSize),
		},
	}

	// Set these methods on base with our custom versions.
	// These methods are not defined at all in base. Failing
	// to set them will result in nil pointers and crashes.
	deleter.Base.ShouldSkipThis = deleter.ShouldSkipThis
	deleter.Base.GetTaskObject = deleter.GetTaskObject

	deleter.Context.Logger.Info("Delete worker started with the following settings:")
	deleter.Context.Logger.Info(settings.ToJSON())
	deleter.Context.Logger.Info("Config settings (omitting sensitive credentials):")
	deleter.Context.Logger.Info(deleter.Context.Config.ToJSON())

	// Spin up the go routines that will act as workers
	for i := 0; i < settings.NumberOfWorkers; i++ {
		deleter.Context.Logger.Infof("Starting worker #%d", i+1)
		go deleter.ProcessItem()
	}
	go deleter.ProcessErrorChannel()
	go deleter.ProcessFatalErrorChannel()
	go deleter.ProcessSuccessChannel()

	return deleter
}

func (d *Deleter) ProcessSuccessChannel() {
	for task := range d.SuccessChannel {
		d.Context.Logger.Infof("WorkItem %d (%s) is in success channel",
			task.WorkItem.ID, task.WorkItem.Name)

		// Tell Pharos item succeeded.
		note := fmt.Sprintf("Deletion completed at the request of %s, approved by %s.", task.WorkItem.User, task.WorkItem.InstApprover)
		if task.WorkItem.APTrustApprover != "" {
			note += fmt.Sprintf(" APTrust approver: %s.", task.WorkItem.APTrustApprover)
		}
		task.WorkItem.Note = note
		task.WorkItem.Stage = d.Settings.NextWorkItemStage
		task.WorkItem.Status = constants.StatusSuccess
		task.WorkItem.Retry = false
		task.WorkItem.NeedsAdminReview = false

		d.FinishItem(task)

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

		// Update Pharos and Redis
		d.FinishItem(task)

		// Tell NSQ we're done with this message.
		task.NSQFinish()
	}
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
	if d.ShouldRetry(workItem) == false {
		return true
	}

	// Definitely don't delete this if it's not a deletion request.
	if d.HasWrongAction(workItem) {
		return true
	}

	// Do not proceed without the approval of institutional admin.
	if d.MissingRequiredApproval(workItem) {
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

// HasWrongAction returns true and marks this item as no longer in
// progress if the WorkItem.Action is anything other than delete.
func (d *Deleter) HasWrongAction(workItem *registry.WorkItem) bool {
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
	return false
}

// MissingRequiredApproval returns true and marks this item as no longer in
// progress if the deletion WorkItem has not been approved by an institutional
// admin.
func (d *Deleter) MissingRequiredApproval(workItem *registry.WorkItem) bool {
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
	return false
}
