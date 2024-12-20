package workers

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/restoration"
	"github.com/nsqio/go-nsq"
)

// GlacierRestorer initiates and checks on the progress of Glacier restoration
// requests. When requests are complete, the Glacier items are in S3 and
// can be retrieved.
//
// Note that requeing is part of this worker's standard process. It makes an
// initial restore request, then checks Glacier every four hours to see if
// the request has been completed.
type GlacierRestorer struct {
	Base
}

// NewGlacierRestorer creates a new GlacierRestorer worker. Param context is a
// Context object with connections to S3, Redis, Registry, and NSQ.
func NewGlacierRestorer(bufSize, numWorkers, maxAttempts int) *GlacierRestorer {
	_context := common.NewContext()
	bufSize, numWorkers, maxAttempts = _context.Config.GetWorkerSettings(constants.TopicGlacierRestore, bufSize, numWorkers, maxAttempts)
	settings := &Settings{
		ChannelBufferSize: bufSize,
		MaxAttempts:       maxAttempts,
		NSQChannel:        constants.TopicGlacierRestore + "_worker_chan",
		NSQTopic:          constants.TopicGlacierRestore,
		NextQueueTopic:    "",
		NextWorkItemStage: constants.StageResolve,
		NumberOfWorkers:   numWorkers,
		RequeueTimeout:    (4 * time.Hour),
	}
	restorer := &GlacierRestorer{
		Base: Base{
			Context:           _context,
			Settings:          settings,
			ItemsInProcess:    service.NewRingList(settings.ChannelBufferSize * settings.NumberOfWorkers),
			ProcessChannel:    make(chan *Task, settings.ChannelBufferSize),
			SuccessChannel:    make(chan *Task, settings.ChannelBufferSize),
			ErrorChannel:      make(chan *Task, settings.ChannelBufferSize),
			FatalErrorChannel: make(chan *Task, settings.ChannelBufferSize),
			KillChannel:       make(chan os.Signal, 1),
		},
	}

	// Handle SIGTERM & SIGINT
	signal.Notify(restorer.KillChannel, syscall.SIGTERM, syscall.SIGINT)

	// Set these methods on base with our custom versions.
	// These methods are not defined at all in base. Failing
	// to set them will result in nil pointers and crashes.
	restorer.Base.ShouldSkipThis = restorer.ShouldSkipThis
	restorer.Base.GetTaskObject = restorer.GetTaskObject

	restorer.Context.Logger.Info("Glacier Restorer started with the following settings:")
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

func (r *GlacierRestorer) ProcessSuccessChannel() {
	for task := range r.SuccessChannel {
		r.Context.Logger.Infof("WorkItem %d (%s) is in success channel",
			task.WorkItem.ID, task.WorkItem.Name)

		// Tell Registry item succeeded.
		note := fmt.Sprintf("Object %s restored from Glacier to S3.", task.WorkItem.ObjectIdentifier)
		task.WorkItem.Note = note
		task.WorkItem.Stage = r.Settings.NextWorkItemStage
		task.WorkItem.Status = constants.StatusSuccess
		task.WorkItem.Retry = false
		task.WorkItem.NeedsAdminReview = false

		r.FinishItem(task)

		// Once Glacier Restoration is complete, we create a normal
		// restoration WorkItem. Since all files are now in S3, we
		// can follow the normal restoration workflow.
		//
		// We don't handle error here. The function logs it internally.
		workItem, _ := r.CreateRestorationWorkItem(task)

		// Add the WorkItem to NSQ.
		if workItem != nil {
			if task.RestorationObject.RestorationType == constants.RestorationTypeFile {
				r.Context.NSQClient.Enqueue(constants.TopicFileRestore, workItem.ID)
			} else {
				r.Context.NSQClient.Enqueue(constants.TopicObjectRestore, workItem.ID)
			}
		}

		// Tell NSQ this worker is done with this message.
		task.NSQFinish()
	}
}

func (r *GlacierRestorer) ProcessErrorChannel() {
	for task := range r.ErrorChannel {
		shouldRequeue := true
		r.Context.Logger.Warningf("WorkItem %d (%s) is in error channel",
			task.WorkItem.ID, task.WorkItem.Name)
		r.Context.Logger.Warningf("Non-fatal errors for WorkItem %d (%s): %s",
			task.WorkItem.ID, task.WorkItem.Name,
			task.WorkResult.NonFatalErrorMessage())

		// Update WorkItem in Registry
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

func (r *GlacierRestorer) ProcessFatalErrorChannel() {
	for task := range r.FatalErrorChannel {
		r.Context.Logger.Errorf("WorkItem %d (%s) is in fatal error channel",
			task.WorkItem.ID, task.WorkItem.Name)
		r.Context.Logger.Errorf("Fatal errors for WorkItem %d (%s): %s",
			task.WorkItem.ID, task.WorkItem.Name,
			task.WorkResult.FatalErrorMessage())

		// Update WorkItem for Registry
		task.WorkItem.Note = task.WorkResult.FatalErrorMessage()
		task.WorkItem.Retry = false
		task.WorkItem.NeedsAdminReview = true

		// Update Registry and Redis
		r.FinishItem(task)

		// Tell NSQ we're done with this message.
		task.NSQFinish()
	}
}

func (r *GlacierRestorer) GetTaskObject(message *nsq.Message, workItem *registry.WorkItem, workResult *service.WorkResult) (*Task, error) {

	restorationObject, err := GetRestorationObject(r.Context, workItem, constants.RestorationSourceGlacier)
	if err != nil {
		return nil, err
	}

	restorer := restoration.NewGlacierRestorer(r.Context, workItem.ID, restorationObject)

	// Set up the restoration item, which packages all the info
	// that needs to be passed from channel to channel.
	task := &Task{
		Processor:         restorer,
		NSQMessage:        message,
		RestorationObject: restorationObject,
		WorkItem:          workItem,
		WorkResult:        workResult,
	}
	return task, nil
}

// ShouldSkipThis returns true if there are any reasons not process this
// WorkItem.
func (r *GlacierRestorer) ShouldSkipThis(workItem *registry.WorkItem) bool {

	// It's possible that another worker recently marked this as
	// "do not retry." If that's the case, skip it.
	if !r.ShouldRetry(workItem) {
		return true
	}

	// Make sure this is actually a restoration request
	if HasWrongAction(r.Context, workItem, constants.ActionGlacierRestore) {
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

// CreateRestorationWorkItem creates a new WorkItem in Registry to restore
// the file(s) we just worked on. Glacier restoration is a two-step process,
// with this worker handling the first step of the process, which is to
// copy files from Glacier to S3. When our step is done, we create a WorkItem
// saying the file or object is ready to go into the normal restoration process,
// moving from S3 through packaging to the depositor's restoration bucket.
func (r *GlacierRestorer) CreateRestorationWorkItem(task *Task) (*registry.WorkItem, error) {
	action := constants.ActionRestoreObject
	if task.WorkItem.GenericFileID > 0 {
		action = constants.ActionRestoreFile
	}
	newItem := &registry.WorkItem{
		Action:               action,
		BagDate:              task.WorkItem.BagDate,
		Bucket:               task.WorkItem.Bucket,
		DateProcessed:        task.WorkItem.DateProcessed,
		ETag:                 task.WorkItem.ETag,
		GenericFileID:        task.WorkItem.GenericFileID,
		InstitutionID:        task.WorkItem.InstitutionID,
		IntellectualObjectID: task.WorkItem.IntellectualObjectID,
		Name:                 task.WorkItem.Name,
		Note:                 "Moved from Glacier to S3, awaiting restoration",
		ObjectIdentifier:     task.WorkItem.ObjectIdentifier,
		Outcome:              "Moved from Glacier to S3, awaiting restoration",
		Retry:                true,
		Size:                 task.WorkItem.Size,
		Stage:                constants.StageRequested,
		Status:               constants.StatusPending,
		User:                 task.WorkItem.User,
	}
	resp := r.Context.RegistryClient.WorkItemSave(newItem)
	if resp.Error != nil {
		r.Context.Logger.Errorf("Error saving restoration WorkItem to Registry for %s: %v", task.RestorationObject.Identifier, resp.Error)
		task.WorkItem.Note = task.WorkItem.Note + " Object(s) are in S3 but worker was unable to create next restore item. Create it manually."
		task.WorkItem.NeedsAdminReview = true
		resp = r.Context.RegistryClient.WorkItemSave(task.WorkItem)
		if resp.Error != nil {
			r.Context.Logger.Errorf("Error flagging WorkItem in Registry for %s: %v", task.RestorationObject.Identifier, resp.Error)
		}
		return nil, resp.Error
	} else {
		r.Context.Logger.Infof("Created new WorkItem in Registry with ID %s to restore %s from S3 to depositor bucket.", resp.WorkItem().ID, task.RestorationObject.Identifier)
	}
	return resp.WorkItem(), resp.Error
}
