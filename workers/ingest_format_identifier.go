package workers

import (
	"fmt"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

type FormatIdentifier struct {
	*IngestBase
}

// NewFormatIdentifier creates a new FormatIdentifier worker.
func NewIngestFormatIdentifier(bufSize, numWorkers, maxAttempts int) *FormatIdentifier {
	_context := common.NewContext()
	bufSize, numWorkers, maxAttempts = _context.Config.GetWorkerSettings(constants.IngestFormatIdentification, bufSize, numWorkers, maxAttempts)
	settings := &Settings{
		ChannelBufferSize:                         bufSize,
		DeleteFromReceivingAfterFatalError:        false,
		DeleteFromReceivingAfterMaxFailedAttempts: false,
		MaxAttempts:                         maxAttempts,
		NSQChannel:                          constants.IngestFormatIdentification + "_worker_chan",
		NSQTopic:                            constants.IngestFormatIdentification,
		NextQueueTopic:                      constants.IngestStorage,
		NextWorkItemStage:                   constants.StageStore,
		NumberOfWorkers:                     numWorkers,
		PushToCleanupAfterMaxFailedAttempts: false,
		PushToCleanupOnFatalError:           false,
		RequeueTimeout:                      (1 * time.Minute),
		WorkItemSuccessNote:                 "Finished file format identification",
	}
	worker := &FormatIdentifier{
		IngestBase: NewIngestBase(
			_context,
			createFormatIdentifier,
			settings,
		),
	}

	err := worker.IngestBase.RegisterAsNsqConsumer()
	if err != nil {
		panic(fmt.Sprintf("Cannot register NSQ consumer: %v", err))
	}
	return worker
}

func createFormatIdentifier(context *common.Context, workItemID int, ingestObject *service.IngestObject) ingest.Runnable {
	return ingest.NewFormatIdentifier(context, workItemID, ingestObject)
}
