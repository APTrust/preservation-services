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

// NewIngestPreFetch creates a new IngestPreFetch worker. The worker starts
// handling NSQ messages as soon as it's instantiated.
func NewIngestPreFetch(bufSize, numWorkers, maxAttempts int) *IngestPreFetch {
	settings := &Settings{
		ChannelBufferSize:                         bufSize,
		DeleteFromReceivingAfterFatalError:        false,
		DeleteFromReceivingAfterMaxFailedAttempts: false,
		MaxAttempts:                         maxAttempts,
		NSQChannel:                          constants.IngestPreFetch + "_worker_chan",
		NSQTopic:                            constants.IngestPreFetch,
		NextQueueTopic:                      constants.IngestValidation,
		NextWorkItemStage:                   constants.StageValidate,
		NumberOfWorkers:                     numWorkers,
		PushToCleanupAfterMaxFailedAttempts: false,
		PushToCleanupOnFatalError:           false,
		RequeueTimeout:                      (1 * time.Minute),
		WorkItemSuccessNote:                 "Finished pre-fetch metadata gathering",
	}
	worker := &IngestPreFetch{
		IngestBase: NewIngestBase(
			common.NewContext(),
			createMetadataGatherer,
			settings,
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

func createMetadataGatherer(context *common.Context, workItemID int, ingestObject *service.IngestObject) ingest.Runnable {
	return ingest.NewMetadataGatherer(context, workItemID, ingestObject)
}
