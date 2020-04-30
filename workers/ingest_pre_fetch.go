package workers

import (
	"fmt"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

type IngestPreFetch struct {
	*IngestBase
}

// NewIngestPreFetch creates a new IngestPreFetch worker.
func NewIngestPreFetch(bufSize, numWorkers, maxAttempts int) *IngestPreFetch {
	worker := &IngestPreFetch{
		IngestBase: NewIngestBase(
			common.NewContext(),
			createMetadataGatherer,
			bufSize,
			numWorkers,
			maxAttempts,
			constants.IngestPreFetch,
		),
	}

	// The underlying IngestBase worker will start handling messages
	// as soon as you call this. IngestBase pushes items into the
	// ProcessChannel, and from there to SuccessChannel, ErrorChannel,
	// or FatalErrorChannel.
	err := worker.IngestBase.RegisterAsNsqConsumer()
	if err != nil {
		panic(fmt.Sprintf("Cannot register NSQ consumer: %v", err))
	}
	return worker
}

func (worker *IngestPreFetch) ProcessSuccessChannel() {
	for ingestItem := range worker.SuccessChannel {
		// Tell Pharos item succeeded.
		ingestItem.WorkItem.Note = "Completed pre-fetch data gathering."
		ingestItem.WorkItem.Stage = constants.StageValidate
		ingestItem.WorkItem.Status = constants.StatusPending
		ingestItem.WorkItem.Retry = true
		ingestItem.WorkItem.NeedsAdminReview = false

		// Push item to next queue.
		ingestItem.NextQueueTopic = constants.IngestValidation
		worker.FinishItem(ingestItem)

		// Tell NSQ this worker is done with this message.
		ingestItem.NSQFinish()
	}
}

func (worker *IngestPreFetch) ProcessErrorChannel() {
	for ingestItem := range worker.ErrorChannel {
		shouldRequeue := true

		// Update WorkItem in Pharos
		ingestItem.WorkItem.Note = ingestItem.WorkResult.NonFatalErrorMessage()
		if ingestItem.WorkResult.Attempt >= worker.MaxAttempts {
			ingestItem.WorkItem.Note += fmt.Sprintf(" Will not retry: failed %d times. Interim processing data persists.", ingestItem.WorkResult.Attempt)
			ingestItem.WorkItem.Retry = false
			ingestItem.WorkItem.NeedsAdminReview = true
			shouldRequeue = false
		}

		// Clear this, so the item is not pushed to the next queue
		ingestItem.NextQueueTopic = ""

		worker.FinishItem(ingestItem)
		if shouldRequeue {
			ingestItem.NSQRequeue(1 * time.Minute)
		} else {
			ingestItem.NSQFinish()
		}
	}
}

func (worker *IngestPreFetch) ProcessFatalErrorChannel() {
	for ingestItem := range worker.FatalErrorChannel {
		// Update WorkItem for Pharos
		ingestItem.WorkItem.Note = ingestItem.WorkResult.FatalErrorMessage()
		ingestItem.WorkItem.Retry = false
		ingestItem.WorkItem.NeedsAdminReview = true

		// Push into NSQ cleanup topic.
		ingestItem.NextQueueTopic = constants.IngestCleanup
		worker.FinishItem(ingestItem)

		// Tell NSQ we're done with this message.
		ingestItem.NSQFinish()
	}
}

func createMetadataGatherer(context *common.Context, workItemID int, ingestObject *service.IngestObject) ingest.Runnable {
	return ingest.NewMetadataGatherer(context, workItemID, ingestObject)
}
