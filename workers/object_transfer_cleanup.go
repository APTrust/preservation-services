package workers

import (
	"fmt"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/transfer"
)

type ObjectTransferCleanup struct {
	*TransferBase
}

// NewIngestCleanup creates a new IngestCleanup worker.
func NewObjectTransferCleanup(bufSize, numWorkers, maxAttempts int) *ObjectTransferCleanup {
	_context := common.NewContext()
	bufSize, numWorkers, maxAttempts = _context.Config.GetWorkerSettings(constants.IngestCleanup, bufSize, numWorkers, maxAttempts)
	settings := &Settings{
		ChannelBufferSize:                         bufSize,
		DeleteFromReceivingAfterFatalError:        false,
		DeleteFromReceivingAfterMaxFailedAttempts: false,
		MaxAttempts:                         maxAttempts,
		NSQChannel:                          constants.IngestCleanup + "_worker_chan",
		NSQTopic:                            constants.IngestCleanup,
		NextQueueTopic:                      "",
		NextWorkItemStage:                   constants.StageTransferCleanup,
		NumberOfWorkers:                     numWorkers,
		PushToCleanupAfterMaxFailedAttempts: false,
		PushToCleanupOnFatalError:           false,
		RequeueTimeout:                      (1 * time.Minute),
		WorkItemSuccessNote:                 "Finished cleanup. Ingest complete.",
	}
	worker := &ObjectTransferCleanup{
		TransferBase: NewTransferBase(
			_context,
			createObjectTransferCleanup,
			settings,
		),
	}

	err := worker.TransferBase.RegisterAsNsqConsumer()
	if err != nil {
		panic(fmt.Sprintf("Cannot register NSQ consumer: %v", err))
	}
	return worker
}

func createObjectTransferCleanup(context *common.Context, workItemID int64, transferObject *service.TransferObject) transfer.Runnable {
	return transfer.NewObjectTransferCleanup(context, workItemID, transferObject)
}
