package workers

import (
	"fmt"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

type ReingestManager struct {
	*IngestBase
}

// NewReingestManager creates a new ReingestManager worker.
func NewReingestManager(bufSize, numWorkers, maxAttempts int) *ReingestManager {
	_context := common.NewContext()
	bufSize, numWorkers, maxAttempts = _context.Config.GetWorkerSettings(constants.IngestReingestCheck, bufSize, numWorkers, maxAttempts)
	settings := &Settings{
		ChannelBufferSize:                         bufSize,
		DeleteFromReceivingAfterFatalError:        false,
		DeleteFromReceivingAfterMaxFailedAttempts: false,
		MaxAttempts:                         maxAttempts,
		NSQChannel:                          constants.IngestReingestCheck + "_worker_chan",
		NSQTopic:                            constants.IngestReingestCheck,
		NextQueueTopic:                      constants.IngestStaging,
		NextWorkItemStage:                   constants.StageCopyToStaging,
		NumberOfWorkers:                     numWorkers,
		PushToCleanupAfterMaxFailedAttempts: false,
		PushToCleanupOnFatalError:           false,
		RequeueTimeout:                      (1 * time.Minute),
		WorkItemSuccessNote:                 "Finished reingest check",
	}
	worker := &ReingestManager{
		IngestBase: NewIngestBase(
			_context,
			createReingestManager,
			settings,
		),
	}

	err := worker.IngestBase.RegisterAsNsqConsumer()
	if err != nil {
		panic(fmt.Sprintf("Cannot register NSQ consumer: %v", err))
	}
	return worker
}

func createReingestManager(context *common.Context, workItemID int, ingestObject *service.IngestObject) ingest.Runnable {
	return ingest.NewReingestManager(context, workItemID, ingestObject)
}
