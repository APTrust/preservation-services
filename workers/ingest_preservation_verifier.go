package workers

import (
	"fmt"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

type PreservationVerifier struct {
	*IngestBase
}

// NewIngestPreservationVerifier creates a new PreservationVerifier worker
// to verify that files have been correctly copied to preservation
// (and replication) storage.
func NewIngestPreservationVerifier(bufSize, numWorkers, maxAttempts int) *PreservationVerifier {
	_context := common.NewContext()
	bufSize, numWorkers, maxAttempts = _context.Config.GetWorkerSettings(constants.IngestStorageValidation, bufSize, numWorkers, maxAttempts)
	settings := &Settings{
		ChannelBufferSize:                         bufSize,
		DeleteFromReceivingAfterFatalError:        false,
		DeleteFromReceivingAfterMaxFailedAttempts: false,
		MaxAttempts:                         maxAttempts,
		NSQChannel:                          constants.IngestStorageValidation + "_worker_chan",
		NSQTopic:                            constants.IngestStorageValidation,
		NextQueueTopic:                      constants.IngestRecord,
		NextWorkItemStage:                   constants.StageRecord,
		NumberOfWorkers:                     numWorkers,
		PushToCleanupAfterMaxFailedAttempts: false,
		PushToCleanupOnFatalError:           false,
		RequeueTimeout:                      (1 * time.Minute),
		WorkItemSuccessNote:                 "Finished verifying files in preservation storage",
	}
	worker := &PreservationVerifier{
		IngestBase: NewIngestBase(
			_context,
			createPreservationVerifier,
			settings,
		),
	}

	err := worker.IngestBase.RegisterAsNsqConsumer()
	if err != nil {
		panic(fmt.Sprintf("Cannot register NSQ consumer: %v", err))
	}
	return worker
}

func createPreservationVerifier(context *common.Context, workItemID int, ingestObject *service.IngestObject) ingest.Runnable {
	return ingest.NewPreservationVerifier(context, workItemID, ingestObject)
}
