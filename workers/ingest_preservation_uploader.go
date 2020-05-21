package workers

import (
	"fmt"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

type PreservationUploader struct {
	*IngestBase
}

// NewIngestPreservationUploader creates a new PreservationUploader worker.
func NewIngestPreservationUploader(bufSize, numWorkers, maxAttempts int) *PreservationUploader {
	settings := &Settings{
		ChannelBufferSize:                         bufSize,
		DeleteFromReceivingAfterFatalError:        false,
		DeleteFromReceivingAfterMaxFailedAttempts: false,
		MaxAttempts:                         maxAttempts,
		NSQChannel:                          constants.IngestStorage + "_worker_chan",
		NSQTopic:                            constants.IngestStorage,
		NextQueueTopic:                      constants.IngestStorageValidation,
		NextWorkItemStage:                   constants.StageStorageValidation,
		NumberOfWorkers:                     numWorkers,
		PushToCleanupAfterMaxFailedAttempts: false,
		PushToCleanupOnFatalError:           false,
		RequeueTimeout:                      (1 * time.Minute),
		WorkItemSuccessNote:                 "Finished copying files to preservation storage",
	}
	worker := &PreservationUploader{
		IngestBase: NewIngestBase(
			common.NewContext(),
			createPreservationUploader,
			settings,
		),
	}

	err := worker.IngestBase.RegisterAsNsqConsumer()
	if err != nil {
		panic(fmt.Sprintf("Cannot register NSQ consumer: %v", err))
	}
	return worker
}

func createPreservationUploader(context *common.Context, workItemID int, ingestObject *service.IngestObject) ingest.Runnable {
	return ingest.NewPreservationUploader(context, workItemID, ingestObject)
}
