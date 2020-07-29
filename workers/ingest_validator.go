package workers

import (
	"fmt"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

type IngestValidator struct {
	*IngestBase
}

// NewIngestValidator creates a new IngestValidator worker.
func NewIngestValidator(bufSize, numWorkers, maxAttempts int) *IngestValidator {
	settings := &Settings{
		ChannelBufferSize:                         bufSize,
		DeleteFromReceivingAfterFatalError:        true,
		DeleteFromReceivingAfterMaxFailedAttempts: false,
		MaxAttempts:                         maxAttempts,
		NSQChannel:                          constants.IngestValidation + "_worker_chan",
		NSQTopic:                            constants.IngestValidation,
		NextQueueTopic:                      constants.IngestReingestCheck,
		NextWorkItemStage:                   constants.StageReingestCheck,
		NumberOfWorkers:                     numWorkers,
		PushToCleanupAfterMaxFailedAttempts: false,
		PushToCleanupOnFatalError:           true,
		RequeueTimeout:                      (1 * time.Minute),
		WorkItemSuccessNote:                 "Bag is valid",
	}
	worker := &IngestValidator{
		IngestBase: NewIngestBase(
			common.NewContext(),
			createMetadataValidator,
			settings,
		),
	}

	err := worker.IngestBase.RegisterAsNsqConsumer()
	if err != nil {
		panic(fmt.Sprintf("Cannot register NSQ consumer: %v", err))
	}
	return worker
}

func createMetadataValidator(context *common.Context, workItemID int, ingestObject *service.IngestObject) ingest.Runnable {
	return ingest.NewMetadataValidator(context, workItemID, ingestObject)
}
