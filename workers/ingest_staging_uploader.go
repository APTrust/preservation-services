package workers

import (
	"fmt"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

type StagingUploader struct {
	*IngestBase
}

// NewStagingUploader creates a new StagingUploader worker.
func NewStagingUploader(bufSize, numWorkers, maxAttempts int) *StagingUploader {
	settings := &Settings{
		ChannelBufferSize:                         bufSize,
		DeleteFromReceivingAfterFatalError:        false,
		DeleteFromReceivingAfterMaxFailedAttempts: false,
		MaxAttempts:                         maxAttempts,
		NSQChannel:                          constants.IngestStaging + "_worker_chan",
		NSQTopic:                            constants.IngestStaging,
		NextQueueTopic:                      constants.IngestFormatIdentification,
		NextWorkItemStage:                   constants.StageFormatIdentification,
		NumberOfWorkers:                     numWorkers,
		PushToCleanupAfterMaxFailedAttempts: false,
		PushToCleanupOnFatalError:           false,
		RequeueTimeout:                      (1 * time.Minute),
		WorkItemSuccessNote:                 "Finished copying files to staging area",
	}
	worker := &StagingUploader{
		IngestBase: NewIngestBase(
			common.NewContext(),
			createStagingUploader,
			settings,
		),
	}

	err := worker.IngestBase.RegisterAsNsqConsumer()
	if err != nil {
		panic(fmt.Sprintf("Cannot register NSQ consumer: %v", err))
	}
	return worker
}

func createStagingUploader(context *common.Context, workItemID int, ingestObject *service.IngestObject) ingest.Runnable {
	return ingest.NewStagingUploader(context, workItemID, ingestObject)
}
