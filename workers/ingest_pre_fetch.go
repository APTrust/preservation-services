package workers

import (
	"fmt"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	//"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	//"github.com/nsqio/go-nsq"
)

type IngestPreFetch struct {
	IngestBase
}

// NewIngestPreFetch creates a new IngestPreFetch worker.
func NewIngestPreFetch(bufSize, numWorkers int) *IngestPreFetch {
	worker := &IngestPreFetch{
		IngestBase: NewIngestBase(
			common.NewContext(),
			createMetadataGatherer,
			bufSize,
			numWorkers,
			constants.IngestPreFetch,
		),
	}

	err := worker.IngestBase.RegisterAsNsqConsumer()
	if err != nil {
		panic(fmt.Sprintf("Cannot register NSQ consumer: %v", err))
	}
	return worker
}

func (worker *IngestPreFetch) ProcessSuccessChannel() {
	for ingestItem := range worker.SuccessChannel {
		// Tell Pharos item succeeded.
		// Finish NSQ message.
		// Push item to next queue.
		worker.Context.Logger.Info(ingestItem.WorkItem.ID)
	}
}

func (worker *IngestPreFetch) ProcessErrorChannel() {
	for ingestItem := range worker.ErrorChannel {
		// Add non-fatal error to Pharos WorkItem.Note.
		// Requeue in NSQ with some delay.

		// If we passed max attempts, mark item as
		// failed in Pharos and set NeedsAdminReview = true.
		// Then mark item finished in NSQ.
		// Do not push to next queue.
		worker.Context.Logger.Info(ingestItem.WorkItem.ID)
	}
}

func (worker *IngestPreFetch) ProcessFatalErrorChannel() {
	for ingestItem := range worker.FatalErrorChannel {
		// Mark Pharos WorkItem.Note with error message.
		// Mark Pharos WorkItem as failed with Retry = false
		// If error is not a bag validation error, set
		// NeedsAdminReview to true.
		// Push to Cleanup queue
		worker.Context.Logger.Info(ingestItem.WorkItem.ID)
	}
}

func createMetadataGatherer(context *common.Context, workItemID int, ingestObject *service.IngestObject) ingest.Runnable {
	return ingest.NewMetadataGatherer(context, workItemID, ingestObject)
}
