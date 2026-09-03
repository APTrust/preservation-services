package workers

import (
	"fmt"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/transfer"
)

type ObjectTransferValidator struct {
	*TransferBase
}

// NewIngestPreservationVerifier creates a new PreservationVerifier worker
// to verify that files have been correctly copied to preservation
// (and replication) storage.
func NewObjectTransferValidator(bufSize, numWorkers, maxAttempts int) *ObjectTransferValidator {
	_context := common.NewContext()
	bufSize, numWorkers, maxAttempts = _context.Config.GetWorkerSettings(constants.TransferValidator, bufSize, numWorkers, maxAttempts)
	settings := &Settings{
		ChannelBufferSize:                         bufSize,
		DeleteFromReceivingAfterFatalError:        false,
		DeleteFromReceivingAfterMaxFailedAttempts: false,
		MaxAttempts:                         maxAttempts,
		NSQChannel:                          constants.TransferValidator + "_worker_chan",
		NSQTopic:                            constants.TransferValidator,
		NextQueueTopic:                      constants.TransferCleanup,
		NextWorkItemStage:                   constants.StageTransferCleanup,
		NumberOfWorkers:                     numWorkers,
		PushToCleanupAfterMaxFailedAttempts: false,
		PushToCleanupOnFatalError:           false,
		RequeueTimeout:                      (1 * time.Minute),
		WorkItemSuccessNote:                 "Finished verifying files in preservation storage",
	}
	worker := &ObjectTransferValidator{
		TransferBase: NewTransferBase(
			_context,
			createObjectTransferValidator,
			settings,
		),
	}

	err := worker.TransferBase.RegisterAsNsqConsumer()
	if err != nil {
		panic(fmt.Sprintf("Cannot register NSQ consumer: %v", err))
	}
	return worker
}

func createObjectTransferValidator(context *common.Context, workItemID int64, transferObject *service.TransferObject) transfer.Runnable {
	return transfer.NewObjectTransferValidator(context, workItemID, transferObject)
}
