package workers

import (
	"fmt"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/transfer"
)

type ObjectTransferCopier struct {
	*TransferBase
}

// TODO: Start with a Glacier check - if transferring from Glacier or Deep Glacier, we need to do a Glacier Restore first

// NewObjectTransferCopier creates a new TransferCopier worker.
func NewObjectTransferCopier(bufSize, numWorkers, maxAttempts int) *ObjectTransferCopier {
	_context := common.NewContext()
	bufSize, numWorkers, maxAttempts = _context.Config.GetWorkerSettings(constants.TransferCopier, bufSize, numWorkers, maxAttempts)
	settings := &Settings{
		ChannelBufferSize:                         bufSize,
		DeleteFromReceivingAfterFatalError:        false,
		DeleteFromReceivingAfterMaxFailedAttempts: false,
		MaxAttempts:                         maxAttempts,
		NSQChannel:                          constants.TransferCopier + "_worker_chan",
		NSQTopic:                            constants.TransferCopier,
		NextQueueTopic:                      constants.TransferValidator,
		NextWorkItemStage:                   constants.StageValidateTransfer,
		NumberOfWorkers:                     numWorkers,
		PushToCleanupAfterMaxFailedAttempts: false,
		PushToCleanupOnFatalError:           false,
		RequeueTimeout:                      (1 * time.Minute),
		WorkItemSuccessNote:                 "Finished copying files to transfer target",
	}
	worker := &ObjectTransferCopier{
		TransferBase: NewTransferBase(
			_context,
			createObjectTransferCopier,
			settings,
		),
	}

	err := worker.TransferBase.RegisterAsNsqConsumer()
	if err != nil {
		panic(fmt.Sprintf("Cannot register NSQ consumer: %v", err))
	}
	return worker
}

func createObjectTransferCopier(context *common.Context, workItemID int64, transferObject *service.TransferObject) transfer.Runnable {
	return transfer.NewObjectTransferCopier(context, workItemID, transferObject)
}
