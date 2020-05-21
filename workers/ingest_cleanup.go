package workers

import (
	"fmt"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

type IngestCleanup struct {
	*IngestBase
}

// NewIngestCleanup creates a new IngestCleanup worker.
func NewIngestCleanup(bufSize, numWorkers, maxAttempts int) *IngestCleanup {
	settings := &Settings{
		ChannelBufferSize:                         bufSize,
		DeleteFromReceivingAfterFatalError:        false,
		DeleteFromReceivingAfterMaxFailedAttempts: false,
		MaxAttempts:                         maxAttempts,
		NSQChannel:                          constants.IngestCleanup + "_worker_chan",
		NSQTopic:                            constants.IngestCleanup,
		NextQueueTopic:                      "",
		NextWorkItemStage:                   constants.StageCleanup,
		NumberOfWorkers:                     numWorkers,
		PushToCleanupAfterMaxFailedAttempts: false,
		PushToCleanupOnFatalError:           false,
		RequeueTimeout:                      (1 * time.Minute),
		WorkItemSuccessNote:                 "Finished cleanup. Ingest complete.",
	}
	worker := &IngestCleanup{
		IngestBase: NewIngestBase(
			common.NewContext(),
			createIngestCleanup,
			settings,
		),
	}

	err := worker.IngestBase.RegisterAsNsqConsumer()
	if err != nil {
		panic(fmt.Sprintf("Cannot register NSQ consumer: %v", err))
	}
	return worker
}

func createIngestCleanup(context *common.Context, workItemID int, ingestObject *service.IngestObject) ingest.Runnable {
	return ingest.NewCleanup(context, workItemID, ingestObject)
}
