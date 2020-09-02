package workers

import (
	"fmt"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/restoration"
	"github.com/nsqio/go-nsq"
)

type FileRestorer struct {
	Base
}

// NewFileRestorer creates a new FileRestorer worker. Param context is a
// Context object with connections to S3, Redis, Pharos, and NSQ.
func NewFileRestorer(bufSize, numWorkers, maxAttempts int) *FileRestorer {
	settings := &Settings{
		ChannelBufferSize: bufSize,
		MaxAttempts:       maxAttempts,
		NSQChannel:        constants.TopicFileRestore + "_worker_chan",
		NSQTopic:          constants.TopicFileRestore,
		NextQueueTopic:    "",
		NextWorkItemStage: constants.StageResolve,
		NumberOfWorkers:   numWorkers,
		RequeueTimeout:    (1 * time.Minute),
	}
	restorer := &FileRestorer{
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
	restorer.Base.ShouldSkipThis = restorer.ShouldSkipThis
	restorer.Base.GetTaskObject = restorer.GetTaskObject

	restorer.Context.Logger.Info("File Restorer started with the following settings:")
	restorer.Context.Logger.Info(settings.ToJSON())
	restorer.Context.Logger.Info("Config settings (omitting sensitive credentials):")
	restorer.Context.Logger.Info(restorer.Context.Config.ToJSON())

	// Spin up the go routines that will act as workers
	for i := 0; i < settings.NumberOfWorkers; i++ {
		restorer.Context.Logger.Infof("Starting worker #%d", i+1)
		go restorer.ProcessItem()
	}
	go restorer.ProcessErrorChannel()
	go restorer.ProcessFatalErrorChannel()
	go restorer.ProcessSuccessChannel()

	err := restorer.RegisterAsNsqConsumer()
	if err != nil {
		panic(fmt.Sprintf("Cannot register NSQ consumer: %v", err))
	}

	return restorer
}

func (r *FileRestorer) ProcessSuccessChannel() {
	for task := range r.SuccessChannel {
		r.Context.Logger.Infof("WorkItem %d (%s) is in success channel",
			task.WorkItem.ID, task.WorkItem.GenericFileIdentifier)

		// Tell Pharos item succeeded.
		note := fmt.Sprintf("File %s restored to %s.", task.WorkItem.GenericFileIdentifier, task.RestorationObject.URL)
		task.WorkItem.Note = note
		task.WorkItem.Stage = r.Settings.NextWorkItemStage
		task.WorkItem.Status = constants.StatusSuccess
		task.WorkItem.Retry = false
		task.WorkItem.NeedsAdminReview = false

		r.FinishItem(task)

		// Tell NSQ this b is done with this message.
		task.NSQFinish()
	}
}

func (r *FileRestorer) ProcessErrorChannel() {
	for task := range r.ErrorChannel {
		shouldRequeue := true
		r.Context.Logger.Warningf("WorkItem %d (%s) is in error channel",
			task.WorkItem.ID, task.WorkItem.Name)
		r.Context.Logger.Warningf("Non-fatal errors for WorkItem %d (%s): %s",
			task.WorkItem.ID, task.WorkItem.Name,
			task.WorkResult.NonFatalErrorMessage())

		// Update WorkItem in Pharos
		task.WorkItem.Note = task.WorkResult.NonFatalErrorMessage()
		if task.WorkResult.Attempt >= r.Settings.MaxAttempts {
			task.WorkItem.Note += fmt.Sprintf(" Will not retry: failed %d times.", task.WorkResult.Attempt)
			task.WorkItem.Retry = false
			task.WorkItem.NeedsAdminReview = true
			shouldRequeue = false
		}
		r.FinishItem(task)
		if shouldRequeue {
			task.NSQRequeue(r.Settings.RequeueTimeout)
		} else {
			task.NSQFinish()
		}
	}
}

func (r *FileRestorer) ProcessFatalErrorChannel() {
	for task := range r.FatalErrorChannel {
		r.Context.Logger.Errorf("WorkItem %d (%s) is in fatal error channel",
			task.WorkItem.ID, task.WorkItem.Name)
		r.Context.Logger.Errorf("Fatal errors for WorkItem %d (%s): %s",
			task.WorkItem.ID, task.WorkItem.Name,
			task.WorkResult.FatalErrorMessage())

		// Update WorkItem for Pharos
		task.WorkItem.Note = task.WorkResult.FatalErrorMessage()
		task.WorkItem.Retry = false
		task.WorkItem.NeedsAdminReview = true

		// Update Pharos and Redis
		r.FinishItem(task)

		// Tell NSQ we're done with this message.
		task.NSQFinish()
	}
}

func (r *FileRestorer) GetTaskObject(message *nsq.Message, workItem *registry.WorkItem, workResult *service.WorkResult) (*Task, error) {

	restorationObject, err := GetRestorationObject(r.Context, workItem, constants.RestorationSourceS3)
	if err != nil {
		return nil, err
	}

	fileRestorer := restoration.NewFileRestorer(r.Context, workItem.ID, restorationObject)

	// Set up the restoration item, which packages all the info
	// that needs to be passed from channel to channel.
	task := &Task{
		Processor:         fileRestorer,
		NSQMessage:        message,
		RestorationObject: restorationObject,
		WorkItem:          workItem,
		WorkResult:        workResult,
	}
	return task, nil
}

// ShouldSkipThis returns true if there are any reasons not process this
// WorkItem.
func (r *FileRestorer) ShouldSkipThis(workItem *registry.WorkItem) bool {

	// It's possible that another worker recently marked this as
	// "do not retry." If that's the case, skip it.
	if r.ShouldRetry(workItem) == false {
		return true
	}

	// Make sure this is actually a restoration request
	if HasWrongAction(r.Context, workItem, constants.ActionRestore) {
		return true
	}

	// FileRestorer shouldn't process object restorations.
	if IsWrongRestorationType(r.Context, workItem, constants.RestorationTypeFile) {
		return true
	}

	// Occasionally, NSQ will think an item has timed out because
	// it took a long time to record. NSQ sends it to a new worker
	// after the original worker has completed it.
	if workItem.ProcessingHasCompleted() {
		message := fmt.Sprintf("Rejecting WorkItem %d because status is %s", workItem.ID, workItem.Status)
		r.Context.Logger.Info(message)
		return true
	}

	// Note that returning nil tells NSQ that a worker is
	// working on this item, even if it's not us. We don't
	// want to requeue duplicates, and we don't want to return
	// an error, because that's equivalent to FIN/failed.
	if r.OtherWorkerIsHandlingThis(workItem) {
		return true
	}

	// See if this worker is already processing this item.
	// This happens sometimes when NSQ thinks the item has
	// timed out while a worker is validating or storing
	// an object.
	if r.ImAlreadyProcessingThis(workItem) {
		return true
	}

	return false
}
