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

/* ---------------------------------------------------------------------

TODO:

Glacier restorations don't quite follow the normal flow of other workers.
If there are no errors, we need to check the value of
RestorationObject.AllFilesRestored to determine whether to re-check
Glacier in four hours or finish this NSQ task and create a new WorkItem.

Consider a simple conditional in the ProcessSuccess method to requeue
with four hour delay if AllFilesRestored is false, or to finish item if
AllFilesRestored is true.

Also, some of the code in this worker is very similar to code in the
BagRestorer worker and should be factored out. For example:

- HasWrongAction
- IsWrongRestorationType
- GetTaskObject
- GetRestorationObject

Move those to a common file or to a RestorerBase object.

 --------------------------------------------------------------------- */

type GlacierRestorer struct {
	Base
}

// NewGlacierRestorer creates a new GlacierRestorer worker. Param context is a
// Context object with connections to S3, Redis, Pharos, and NSQ.
func NewGlacierRestorer(bufSize, numWorkers, maxAttempts int) *GlacierRestorer {
	settings := &Settings{
		ChannelBufferSize: bufSize,
		MaxAttempts:       maxAttempts,
		NSQChannel:        constants.TopicGlacierRestore + "_worker_chan",
		NSQTopic:          constants.TopicGlacierRestore,
		NextQueueTopic:    "",
		NextWorkItemStage: constants.StageResolve,
		NumberOfWorkers:   numWorkers,
		RequeueTimeout:    (1 * time.Minute),
	}
	restorer := &GlacierRestorer{
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

		// Tell Pharos item succeeded.
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
		r.CreateRestorationWorkItem(task)

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

func (r *GlacierRestorer) ProcessFatalErrorChannel() {
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

func (r *GlacierRestorer) GetTaskObject(message *nsq.Message, workItem *registry.WorkItem, workResult *service.WorkResult) (*Task, error) {

	restorationObject, err := r.GetRestorationObject(workItem)
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

func (r *GlacierRestorer) GetRestorationObject(workItem *registry.WorkItem) (*service.RestorationObject, error) {
	resp := r.Context.PharosClient.IntellectualObjectGet(workItem.ObjectIdentifier)
	if resp.Error != nil {
		return nil, resp.Error
	}
	intelObj := resp.IntellectualObject()
	if intelObj == nil {
		return nil, fmt.Errorf("Pharos returned nil for IntellectualObject %s", workItem.ObjectIdentifier)
	}
	resp = r.Context.PharosClient.InstitutionGet(intelObj.Institution)
	if resp.Error != nil {
		return nil, resp.Error
	}
	institution := resp.Institution()
	if intelObj == nil {
		return nil, fmt.Errorf("Pharos returned nil for Institution %s", intelObj.Institution)
	}

	restorationType := constants.RestorationTypeObject
	if workItem.GenericFileIdentifier != "" {
		restorationType = constants.RestorationTypeFile
	}

	return &service.RestorationObject{
		Identifier:             workItem.ObjectIdentifier,
		BagItProfileIdentifier: intelObj.BagItProfileIdentifier,
		RestorationSource:      constants.RestorationSourceS3,
		RestorationTarget:      institution.RestoreBucket,
		RestorationType:        restorationType,
	}, nil
}

// ShouldSkipThis returns true if there are any reasons not process this
// WorkItem.
func (r *GlacierRestorer) ShouldSkipThis(workItem *registry.WorkItem) bool {

	// It's possible that another worker recently marked this as
	// "do not retry." If that's the case, skip it.
	if r.ShouldRetry(workItem) == false {
		return true
	}

	// Make sure this is actually a restoration request
	if r.HasWrongAction(workItem) {
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

// HasWrongAction returns true and marks this item as no longer in
// progress if the WorkItem.Action is anything other than restore.
func (r *GlacierRestorer) HasWrongAction(workItem *registry.WorkItem) bool {
	if workItem.Action != constants.ActionGlacierRestore {
		message := fmt.Sprintf("Rejecting WorkItem %d because action is %s, not '%s'", workItem.ID, workItem.Action, constants.ActionGlacierRestore)
		workItem.Retry = false
		workItem.MarkNoLongerInProgress(
			workItem.Stage,
			constants.StatusCancelled,
			message,
		)
		r.Context.Logger.Info(message)
		return true
	}
	return false
}

// CreateRestorationWorkItem creates a new WorkItem in Pharos to restore
// the file(s) we just worked on. Glacier restoration is a two-step process,
// with this worker handling the first step of the process, which is to
// copy files from Glacier to S3. When our step is done, we create a WorkItem
// saying the file or object is ready to go into the normal restoration process,
// moving from S3 through packaging to the depositor's restoration bucket.
func (r *GlacierRestorer) CreateRestorationWorkItem(task *Task) {
	newItem := &registry.WorkItem{
		Action:                constants.ActionRestore,
		BagDate:               task.WorkItem.BagDate,
		Bucket:                task.WorkItem.Bucket,
		Date:                  task.WorkItem.Date,
		ETag:                  task.WorkItem.ETag,
		GenericFileIdentifier: task.WorkItem.GenericFileIdentifier,
		InstitutionID:         task.WorkItem.InstitutionID,
		Name:                  task.WorkItem.Name,
		Note:                  "Moved from Glacier to S3, awaiting restoration",
		ObjectIdentifier:      task.WorkItem.ObjectIdentifier,
		Retry:                 true,
		Size:                  task.WorkItem.Size,
		Stage:                 constants.StageRequested,
		Status:                constants.StatusPending,
		User:                  task.WorkItem.User,
	}
	resp := r.Context.PharosClient.WorkItemSave(newItem)
	if resp.Error != nil {
		r.Context.Logger.Errorf("Error saving restoration WorkItem to Pharos for %s: %v", task.RestorationObject.Identifier, resp.Error)
		task.WorkItem.Note = task.WorkItem.Note + " Object(s) are in S3 but worker was unable to create next restore item. Create it manually."
		task.WorkItem.NeedsAdminReview = true
		resp = r.Context.PharosClient.WorkItemSave(task.WorkItem)
		if resp.Error != nil {
			r.Context.Logger.Errorf("Error flagging WorkItem in Pharos for %s: %v", task.RestorationObject.Identifier, resp.Error)
		}
	} else {
		r.Context.Logger.Infof("Created new WorkItem in Pharos with ID %s to restore %s from S3 to depositor bucket.", resp.WorkItem().ID, task.RestorationObject.Identifier)
	}
}
