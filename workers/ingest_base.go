package workers

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"github.com/nsqio/go-nsq"
)

// IngestBase contains the fundamental structures common to all ingest workers.
type IngestBase struct {
	Base
}

// NewIngestBase creates a new IngestBase worker. Param context is a
// Context object with connections to S3, Redis, Pharos, and NSQ.
// Param bufSize describes the size of the queue buffers. The values
// for opnames/topics are listed in constants.IngestOpNames.
func NewIngestBase(context *common.Context, processorConstructor ingest.BaseConstructor, settings *Settings) *IngestBase {
	ingestBase := &IngestBase{
		Base: Base{
			Context:              context,
			Settings:             settings,
			ItemsInProcess:       service.NewRingList(settings.ChannelBufferSize),
			ProcessChannel:       make(chan *Task, settings.ChannelBufferSize),
			SuccessChannel:       make(chan *Task, settings.ChannelBufferSize),
			ErrorChannel:         make(chan *Task, settings.ChannelBufferSize),
			FatalErrorChannel:    make(chan *Task, settings.ChannelBufferSize),
			processorConstructor: processorConstructor,
			institutionCache:     make(map[int]string),
		},
	}

	// Set these methods on base with our custom versions.
	// These methods are not defined at all in base. Failing
	// to set them will result in nil pointers and crashes.
	ingestBase.Base.ShouldSkipThis = ingestBase.ShouldSkipThis
	ingestBase.Base.GetTaskObject = ingestBase.GetTaskObject

	context.Logger.Infof("%s started with the following settings:", settings.NSQTopic)
	context.Logger.Info(settings.ToJSON())
	context.Logger.Info("Config settings (omitting sensitive credentials):")
	context.Logger.Info(context.Config.ToJSON())

	// We typically want 2 or so workers to do the heavy,
	// long-running processing invlolved in IngestItem.Processor.Run().
	// Too many workers, however, can be counterproductive,
	// maxing out cpu, memory, and/or network bandwidth. The
	// Success/Error/FatalError channels do lightweight work that
	// usually takes <2 seconds per item, so a single go routine
	// will suffice for those.
	for i := 0; i < settings.NumberOfWorkers; i++ {
		context.Logger.Infof("Starting worker #%d", i+1)
		go ingestBase.ProcessItem()
	}
	go ingestBase.ProcessErrorChannel()
	go ingestBase.ProcessFatalErrorChannel()
	go ingestBase.ProcessSuccessChannel()

	return ingestBase
}

func (b *IngestBase) ProcessSuccessChannel() {
	for task := range b.SuccessChannel {
		b.Context.Logger.Infof("WorkItem %d (%s) is in success channel",
			task.WorkItem.ID, task.WorkItem.Name)
		// Tell Pharos item succeeded.
		task.WorkItem.Note = b.Settings.WorkItemSuccessNote
		task.WorkItem.Stage = b.Settings.NextWorkItemStage
		task.WorkItem.Status = constants.StatusPending
		task.WorkItem.Retry = true
		task.WorkItem.NeedsAdminReview = false

		// When cleaup succeeds, we need to mark the item as succeeded,
		// unless we're cleaning up an item that was cancelled.
		if b.Settings.NSQTopic == constants.IngestCleanup {
			if task.WasCancelled {
				b.Context.Logger.Infof("WorkItem %d (%s): Cleaned up cancelled item. Leaving status as Cancelled.",
					task.WorkItem.ID, task.WorkItem.Name)
				task.WorkItem.Status = constants.StatusCancelled
			} else {
				task.WorkItem.Status = constants.StatusSuccess
			}
			task.WorkItem.Outcome = "Ingest complete"
			task.WorkItem.ObjectIdentifier = task.Processor.GetIngestObject().Identifier()
		}

		// Push item to next queue.
		task.NextQueueTopic = b.Settings.NextQueueTopic
		b.FinishItem(task)

		// Tell NSQ this worker is done with this message.
		task.NSQFinish()
	}
}

func (b *IngestBase) ProcessErrorChannel() {
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

			// Clear this, so if it's manually requeued,
			// it will get a new set of attempts.
			task.WorkResult.Attempt = 0

			// Go to NSQ cleanup or not?
			if b.Settings.PushToCleanupAfterMaxFailedAttempts {
				task.Processor.GetIngestObject().ShouldDeleteFromReceiving = b.Settings.DeleteFromReceivingAfterMaxFailedAttempts
				task.Processor.IngestObjectSave()
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

func (b *IngestBase) ProcessFatalErrorChannel() {
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
		task.WorkItem.Status = constants.StatusFailed

		// NSQ
		if b.Settings.PushToCleanupOnFatalError && task.WorkItem.Stage != constants.StageCleanup {
			task.Processor.GetIngestObject().ShouldDeleteFromReceiving = b.Settings.DeleteFromReceivingAfterFatalError
			task.Processor.IngestObjectSave()
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

// GetTaskObject returns an object representing the task to be implemented.
// This object will be passed from channel to channel during processing.
func (b *IngestBase) GetTaskObject(message *nsq.Message, workItem *registry.WorkItem, workResult *service.WorkResult) (*Task, error) {
	ingestObject, err := b.GetIngestObject(workItem)
	if err != nil {
		message := fmt.Sprintf("WorkItem %d: %v", workItem.ID, err)
		b.Context.Logger.Error(message)
		workItem.Note = message
		b.SaveWorkItem(workItem)
		workResult.Attempt++
		b.SaveWorkResult(workItem.ID, workResult)
		return nil, err
	}

	// Set up the Task.
	task := &Task{
		NSQMessage:   message,
		WasCancelled: workItem.Status == constants.StatusCancelled,
		WorkResult:   workResult,
		WorkItem:     workItem,
		Processor:    b.processorConstructor(b.Context, workItem.ID, ingestObject),
	}
	return task, nil
}

// ShouldSkipThis returns true if there are any reasons not process this
// WorkItem.
func (b *IngestBase) ShouldSkipThis(workItem *registry.WorkItem) bool {

	// It's possible that another worker recently marked this as
	// "do not retry." If that's the case, skip it.
	if b.ShouldRetry(workItem) == false {
		return true
	}

	// Occasionally, NSQ will think an item has timed out because
	// it took a long time to record. NSQ sends it to a new worker
	// after the original worker has completed it.
	if workItem.ProcessingHasCompleted() && workItem.Stage != constants.StageCleanup {
		message := fmt.Sprintf("Rejecting WorkItem %d because status is %s", workItem.ID, workItem.Status)
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

// GetIngestObject returns the IngestObject for the specified WorkItem from
// Redis, or it creates a new one. For the first phase of ingest (PreFetch),
// this will almost always have to create a new IngestObject. For subsequent
// phases, it should never have to create one.
func (b *IngestBase) GetIngestObject(workItem *registry.WorkItem) (*service.IngestObject, error) {
	instIdentifier, err := b.GetInstitutionIdentifier(workItem.InstitutionID)
	if err != nil {
		return nil, b.Error(workItem.ID, "", err, true)
	}
	objName := util.StripFileExtension(workItem.Name)
	objIdentifier := fmt.Sprintf("%s/%s", instIdentifier, objName)
	ingestObject, err := b.Context.RedisClient.IngestObjectGet(workItem.ID, objIdentifier)
	if err == nil && ingestObject != nil {
		return ingestObject, nil
	}
	if err != nil && b.Settings.NSQTopic != constants.IngestPreFetch {
		return nil, fmt.Errorf("Ingest object not found in Redis: %v", err)
	}
	return service.NewIngestObject(
		workItem.Bucket,
		workItem.Name,
		workItem.ETag,
		instIdentifier,
		workItem.InstitutionID,
		workItem.Size,
	), nil
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

// IsLateStageOfIngest returns true if we're at or beyound a point in the
// ingest process where all of an object's files have been copied to the
// staging area. At this point most of the heavy work has been done, and
// the ingest workers no longer need to reference the object in the depositor's
// receiving bucket, so it's best to finish the ingest process, even if a newer
// ingest request is pending. If we have to complete this request and the
// newer one, the newer one will simply count as an update/reingest of the
// current object.
func (b *IngestBase) IsLateStageOfIngest() bool {
	return util.StringListContains(constants.LateStagesOfIngest, b.Settings.NSQTopic)
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
			cleanETag := strings.Replace(objInfo.ETag, "\"", "", -1)
			if objInfo.ETag != "" && cleanETag != workItem.ETag {
				message := fmt.Sprintf("Stopping work on WorkItem %d because a newer version of bag %s was found in %s. WorkItem etag='%s', receiving bucket etag='%s'", workItem.ID, workItem.Name, workItem.Bucket, workItem.ETag, cleanETag)
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
