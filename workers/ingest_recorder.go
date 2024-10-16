package workers

import (
	"fmt"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

type IngestRecorder struct {
	*IngestBase
}

// NewIngestRecorder creates a new IngestRecorder worker.
func NewIngestRecorder(bufSize, numWorkers, maxAttempts int) *IngestRecorder {
	_context := common.NewContext()
	bufSize, numWorkers, maxAttempts = _context.Config.GetWorkerSettings(constants.IngestRecord, bufSize, numWorkers, maxAttempts)
	settings := &Settings{
		ChannelBufferSize:                         bufSize,
		DeleteFromReceivingAfterFatalError:        false,
		DeleteFromReceivingAfterMaxFailedAttempts: false,
		MaxAttempts:                         maxAttempts,
		NSQChannel:                          constants.IngestRecord + "_worker_chan",
		NSQTopic:                            constants.IngestRecord,
		NextQueueTopic:                      constants.IngestCleanup,
		NextWorkItemStage:                   constants.StageCleanup,
		NumberOfWorkers:                     numWorkers,
		PushToCleanupAfterMaxFailedAttempts: false,
		PushToCleanupOnFatalError:           false,
		RequeueTimeout:                      (1 * time.Minute),
		WorkItemSuccessNote:                 "Finished recording ingest data in Registry",
	}
	worker := &IngestRecorder{
		IngestBase: NewIngestBase(
			_context,
			createIngestRecorder,
			settings,
		),
	}

	err := worker.IngestBase.RegisterAsNsqConsumer()
	if err != nil {
		panic(fmt.Sprintf("Cannot register NSQ consumer: %v", err))
	}
	return worker
}

func createIngestRecorder(context *common.Context, workItemID int64, ingestObject *service.IngestObject) ingest.Runnable {
	return ingest.NewRecorder(context, workItemID, ingestObject)
}
