package workers

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/APTrust/preservation-services/bagit"
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
// Context object with connections to S3, Redis, Registry, and NSQ.
// Param bufSize describes the size of the queue buffers. The values
// for opnames/topics are listed in constants.IngestOpNames.
func NewIngestBase(context *common.Context, processorConstructor ingest.BaseConstructor, settings *Settings) *IngestBase {
	ingestBase := &IngestBase{
		Base: Base{
			Context:              context,
			Settings:             settings,
			ItemsInProcess:       service.NewRingList(settings.ChannelBufferSize * settings.NumberOfWorkers),
			ProcessChannel:       make(chan *Task, settings.ChannelBufferSize),
			SuccessChannel:       make(chan *Task, settings.ChannelBufferSize),
			ErrorChannel:         make(chan *Task, settings.ChannelBufferSize),
			FatalErrorChannel:    make(chan *Task, settings.ChannelBufferSize),
			KillChannel:          make(chan os.Signal, 1),
			processorConstructor: processorConstructor,
			institutionCache:     make(map[int64]string),
		},
	}

	// Handle SIGTERM & SIGINT
	signal.Notify(ingestBase.KillChannel, syscall.SIGTERM, syscall.SIGINT)

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
		// Tell Registry item succeeded.
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
				task.WorkItem.Outcome = "Ingest cancelled"
			} else {
				task.WorkItem.Status = constants.StatusSuccess
				task.WorkItem.Outcome = "Ingest complete"
				// We shouldn't have to set this, but somehow the cleanup worker
				// keeps picking up completed items. We need to find the root of that.
				task.WorkItem.Retry = false
				task.WorkItem.IntellectualObjectID = task.Processor.IngestObjectGet().ID
				// TEMP
				if task.WorkItem.IntellectualObjectID == 0 {
					panic("ID SHOULD NOT BE ZERO!")
				}
				// END TEMP
			}
		}

		// Push item to next queue.
		task.NextQueueTopic = b.Settings.NextQueueTopic
		b.FinishItem(task)

		// Tell NSQ this worker is done with this message.
		task.NSQFinish()

		// For e2e tests, let the test worker know this succeeded
		b.QueueE2E(task)
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

		// Update WorkItem in Registry
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
				task.Processor.IngestObjectGet().ShouldDeleteFromReceiving = b.Settings.DeleteFromReceivingAfterMaxFailedAttempts
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
			// For e2e tests, let the test worker know this failed
			b.QueueE2E(task)
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

		// Update WorkItem for Registry
		task.WorkItem.Note = task.WorkResult.FatalErrorMessage()
		task.WorkItem.Retry = false
		task.WorkItem.NeedsAdminReview = true
		task.WorkItem.Status = constants.StatusFailed
		task.WorkItem.Outcome = "Ingest failed due to fatal error."

		// NSQ
		if b.Settings.PushToCleanupOnFatalError && task.WorkItem.Stage != constants.StageCleanup {
			task.Processor.IngestObjectGet().ShouldDeleteFromReceiving = b.Settings.DeleteFromReceivingAfterFatalError
			task.Processor.IngestObjectSave()
			b.Context.Logger.Errorf("Pushing WorkItem %d (%s) to NSQ cleanup topic due to fatal errors. Delete from receiving bucket = %t",
				task.WorkItem.ID, task.WorkItem.Name, b.Settings.DeleteFromReceivingAfterFatalError)
			task.NextQueueTopic = constants.IngestCleanup
		} else {
			task.NextQueueTopic = ""
		}

		// Update Registry and Redis, and send to next queue if required.
		b.FinishItem(task)

		// Tell NSQ we're done with this message.
		task.NSQFinish()

		// For e2e tests, let the test worker know this failed
		b.QueueE2E(task)
	}
}

// GetTaskObject returns an object representing the task to be implemented.
// This object will be passed from channel to channel during processing.
func (b *IngestBase) GetTaskObject(message *nsq.Message, workItem *registry.WorkItem, workResult *service.WorkResult) (*Task, error) {
	ingestObject, err := b.IngestObjectGet(workItem)
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
	if !b.ShouldRetry(workItem) {
		return true
	}

	if !b.IsCorrectStage(workItem.Stage) {
		b.Context.Logger.Warningf("Rejecting WorkItem %d because its stage is %s.", workItem.ID, workItem.Stage)
		return true
	}

	if b.ObjectAlreadyIngested(workItem) {
		b.Context.Logger.Warningf("Rejecting WorkItem %d because it looks like it has already completed ingest.", workItem.ID)
		b.SaveWorkItem(workItem)
		return true
	}

	// DEBUG
	//if workItem.Stage == constants.StageCleanup {
	//	j, _ := json.Marshal(workItem)
	//	b.Context.Logger.Infof("WORKITEM DEBUG: Completed: %t, JSON: %s", workItem.ProcessingHasCompleted(), string(j))
	//}
	// END DEBUG

	// TEMP - Find the root of this issue and fix it.
	// TODO - Find and fix the root of this issue.
	//
	// **** This is probably due to the filter problem in Registry.
	// **** Try removing this guard.
	if workItem.Stage == constants.StageCleanup {
		ingestObject, err := b.IngestObjectGet(workItem)
		if err != nil {
			b.Context.Logger.Error(err.Error())
			workItem.Note = err.Error()
		}
		if ingestObject == nil || err != nil {
			message := fmt.Sprintf("Rejecting WorkItem %d because Redis has no IngestObject. Ingest may already be complete.", workItem.ID)
			b.Context.Logger.Info(message)
			return true
		}
	}

	// Occasionally, NSQ will think an item has timed out because
	// it took a long time to record. NSQ sends it to a new worker
	// after the original worker has completed it.
	// if workItem.ProcessingHasCompleted() && workItem.Stage != constants.StageCleanup {
	if workItem.ProcessingHasCompleted() {
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

	// There's a newer ingest request in Registry' WorkItems list,
	// and we're not too far along to abandon this.
	if b.SupersededByNewerRequest(workItem) {
		err := b.SaveWorkItem(workItem)
		if err != nil {
			b.Context.Logger.Warningf("Error trying to tell Registry that WorkItem %d should be cancelled because newer bag was uploaded. %v", workItem.ID, err.Error())
		}
		if workItem.Stage != constants.IngestCleanup {
			b.PushToQueue(workItem, constants.IngestCleanup)
		}
		return true
	}

	// In this case, there's a newer version of the bag in
	// the depositor's receiving bucket, and the Registry WorkItems
	// list my not have even picked it up yet.
	//
	// The flag IngestObject.ShouldDeleteFromReceiving stays false
	// unless ingest is complete or the bag is in valid, so the
	// cleanup worker should not delete the newer item from receiving.
	if b.ShouldAbandonForNewerVersion(workItem) {
		b.SaveWorkItem(workItem)
		b.PushToQueue(workItem, constants.IngestCleanup)
		return true
	}

	return false
}

// IsCorrectStage returns true if the WorkItem's stage matches the
// the ingest stage that this worker is supposed to be processing.
func (b *Base) IsCorrectStage(workItemStage string) bool {
	workerStage, err := constants.IngestStageFor(b.Settings.NSQTopic)
	if err != nil {
		b.Context.Logger.Errorf("Constants has no stage info for NSQ topic %s", b.Settings.NSQTopic)
		return false
	}
	b.Context.Logger.Infof("DEBUG: Topic = %s,  WorkerStage = %s, WorkItem.Stage = %s", b.Settings.NSQTopic, workerStage, workItemStage)
	return workerStage == workItemStage
}

// IngestObjectGet returns the IngestObject for the specified WorkItem from
// Redis, or it creates a new one. For the first phase of ingest (PreFetch),
// this will almost always have to create a new IngestObject. For subsequent
// phases, it should never have to create one.
func (b *IngestBase) IngestObjectGet(workItem *registry.WorkItem) (*service.IngestObject, error) {
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
		errMsg := fmt.Sprintf("Ingest object not found in Redis: %v. ", err)
		_, s3Err := b.Context.S3StatObject(constants.StorageProviderAWS, workItem.Bucket, workItem.Name)
		if s3Err != nil && strings.Contains(s3Err.Error(), "key does not exist") {
			errMsg += "Also, the bag is no longer in the receiving bucket. It may have been deleted due to validation failure or completed ingest, or the depositor may have deleted it."
		}
		return nil, errors.New(errMsg)
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
	v.Add("institution_id", fmt.Sprintf("%d", workItem.InstitutionID))
	v.Add("action", constants.ActionIngest)
	v.Add("sort", "date_processed__desc")
	resp := b.Context.RegistryClient.WorkItemList(v)
	if resp.Error != nil {
		b.Context.Logger.Error("Error getting WorkItems list from Registry: %v",
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
		if item.DateProcessed.After(workItem.DateProcessed) && item.ETag != workItem.ETag && !item.ProcessingHasCompleted() {
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
		if item.BagDate.Before(workItem.BagDate) && item.Retry && !item.ProcessingHasCompleted() {
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

// SupersededByNewerRequest returns true if Registry has an ingest request
// for this same item that's newer than the one we're processing AND we're
// not already in a late stage of ingest.
func (b *IngestBase) SupersededByNewerRequest(workItem *registry.WorkItem) bool {
	newerWorkItem := b.FindNewerIngestRequest(workItem)
	if newerWorkItem != nil && !b.IsLateStageOfIngest() {
		message := fmt.Sprintf("Skipping WorkItem %d because a newer version of this bag is waiting to be ingested in WorkItem %d. Staging files and Redis data will remain until APTrust admin cleans them out.", workItem.ID, newerWorkItem.ID)
		b.Context.Logger.Info(message)
		workItem.MarkNoLongerInProgress(
			workItem.Stage,
			constants.StatusCancelled,
			message,
		)
		workItem.Retry = false
		workItem.NeedsAdminReview = true
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
				message := fmt.Sprintf("Stopping work on WorkItem %d because bag %s was deleted from %s. If this item was successfully ingested, push to cleanup.", workItem.ID, workItem.Name, workItem.Bucket)
				b.Context.Logger.Info(message)
				workItem.MarkNoLongerInProgress(
					workItem.Stage,
					constants.StatusSuspended,
					message,
				)
				workItem.Retry = false
				workItem.NeedsAdminReview = true
				return true
			}
			// This should never happen, due to checks at startup that
			// panic if provider is missing.
			if strings.Contains(err.Error(), "No S3 client for provider") {
				message := fmt.Sprintf("Can't check S3 for %s/%s because there's no S3 provider for %s", workItem.Bucket, workItem.Name, constants.StorageProviderAWS)
				b.Context.Logger.Error(message)
				workItem.MarkNoLongerInProgress(
					workItem.Stage,
					constants.StatusSuspended,
					message,
				)
				workItem.Retry = false
				workItem.NeedsAdminReview = true
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

// ObjectAlreadyIngested returns true if the object with this WorkItem's name,
// institution id and etag has already been ingested. This check should catch
// some highly specific race conditions in the queues that occur only during
// times of heavy ingest.
//
// One cause of this problem was apt_queue, an external process that requeuing
// ingest items when it shouldn't have been. Since fixing that, this problem
// has become even more rare, but it still happens. Thus, this method.
func (b *IngestBase) ObjectAlreadyIngested(workItem *registry.WorkItem) bool {
	if workItem.IntellectualObjectID > 0 {
		workItem.MarkNoLongerInProgress(
			constants.StageCleanup,
			constants.StatusSuccess,
			"Cleanup succeeded. Ingest complete. (ObjectAlreadyIngested: Has object id.)",
		)
		workItem.NeedsAdminReview = false
		workItem.Retry = false
		return true
	}

	nameMinusTar := bagit.CleanBagName(workItem.Name)
	values := url.Values{}
	values.Add("institution_id", strconv.FormatInt(workItem.InstitutionID, 10))
	values.Add("etag", workItem.ETag)
	values.Add("updated_at__gteq", workItem.CreatedAt.Format(time.RFC3339))
	values.Add("per_page", "1")
	values.Add("page", "1")
	resp := b.Context.RegistryClient.IntellectualObjectList(values)
	if resp.Error != nil {
		b.Context.Logger.Warningf("ObjectAlreadyIngested() query returned error while trying to get object with etag %s: %v", workItem.ETag, resp.Error)
		return false
	}
	obj := resp.IntellectualObject()
	if obj == nil || obj.ID == 0 {
		return false
	}
	// Now we have a recently updated object with matching inst id and etag.
	// Let's be extra paranoid and make sure it has a matching ingest event
	// before saying this item has been ingested.
	if strings.HasSuffix(obj.Identifier, nameMinusTar) {

		ingestObject, _ := b.IngestObjectGet(workItem)

		eventFilters := url.Values{}
		eventFilters.Add("event_type", constants.EventIngestion)
		eventFilters.Add("intellectual_object_id", strconv.FormatInt(obj.ID, 10))
		eventFilters.Add("generic_file_id__is_null", "true")
		eventFilters.Add("date_time__gteq", workItem.CreatedAt.Format(time.RFC3339))
		eventFilters.Add("outcome", constants.OutcomeSuccess)
		values.Add("per_page", "1")
		values.Add("page", "1")
		resp = b.Context.RegistryClient.PremisEventList(eventFilters)
		if resp.Error != nil {
			b.Context.Logger.Warningf("ObjectAlreadyIngested() query returned error while trying to get object with etag %s: %v", workItem.ETag, resp.Error)
			return false
		}
		// OK, this item was ingested.
		ingestEvent := resp.PremisEvent()
		if (ingestEvent != nil && ingestEvent.ID > 0) && ingestObject == nil {
			b.Context.Logger.Infof("WorkItem %d looks like it has already completed ingest. See IntelObj %d and PremisEvent %s", workItem.ID, obj.ID, ingestEvent.Identifier)

			workItem.MarkNoLongerInProgress(
				constants.StageCleanup,
				constants.StatusSuccess,
				"Cleanup succeeded. Ingest complete. (ObjectAlreadyIngested: Matched object and event.)",
			)
			workItem.IntellectualObjectID = obj.ID
			workItem.NeedsAdminReview = false
			workItem.Retry = false
			return true
		}
	}
	// Else, not ingested yet.
	return false
}

func (b *IngestBase) QueueE2E(task *Task) {
	if b.Context.Config.IsE2ETest() && b.Settings.NextQueueTopic == "" {
		e2eTopic := constants.TopicE2EIngest
		if task.Processor.IngestObjectGet().IsReingest {
			e2eTopic = constants.TopicE2EReingest
		}
		b.Context.Logger.Infof("Pushing %s (%d) into e2e topic %s", task.WorkItem.Name, task.WorkItem.ID, e2eTopic)
		QueueE2EWorkItem(b.Context, e2eTopic, task.WorkItem.ID)
	}
}
