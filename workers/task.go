package workers

import (
	"time"

	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/nsqio/go-nsq"
)

// Task encapsulates everything that a worker will need to
// pass from one channel to the next during procesing.
type Task struct {

	// NextQueueTopic is the name of the next NSQ topic to which
	// this item should be pushed. For ingest, valid values are:
	//
	// constants.IngestPreFetch,
	// constants.IngestValidation,
	// constants.IngestReingestCheck,
	// constants.IngestStaging,
	// constants.IngestFormatIdentification,
	// constants.IngestStorage,
	// constants.IngestStorageValidation,
	// constants.IngestRecord,
	// constants.IngestCleanup.
	//
	// An empty string is also valid, indicating that this item
	// should not be pushed into any NSQ topic.
	NextQueueTopic string

	// NSQMessage is the NSQ message the worker is processing.
	NSQMessage *nsq.Message

	// Processor is handles whatever phase of the ingest process
	// this worker is responsible for (validation, storage, recording, etc.)
	Processor ingest.Runnable

	// WasCancelled will be true if this task was cancelled in any
	// stage prior to Cleanup. When this is true, the Cleanup worker
	// must set the final status of this task's WorkItem to Cancelled
	// instead of Succeeded, even when Cleanup does succeed.
	WasCancelled bool

	// WorkResult describes the result of this worker's work.
	WorkResult *service.WorkResult

	// WorkItem is the Pharos WorkItem that describes the bag, object,
	// of file the worker is working on.
	WorkItem *registry.WorkItem

	nsqStopChannel chan bool

	// For testing
	nsqStartCalled bool

	// For testing
	tickerStopped bool
}

// NSQStart creates a timer that touches the NSQ message
// every two minutes while the WorkItem is in process. We need this
// because operations like calculating checksums on a 200GB file
// cannot pause to touch the NSQ message before it times out.
func (item *Task) NSQStart() {
	item.NSQMessage.DisableAutoResponse()
	interval := time.Duration(2) * time.Minute
	ticker := time.NewTicker(interval)
	stopChannel := make(chan bool)
	go func() {
		for {
			select {
			case <-ticker.C:
				item.NSQMessage.Touch()
			case <-stopChannel:
				ticker.Stop()
				item.tickerStopped = true
				return
			}
		}
	}()
	item.nsqStartCalled = true
	item.nsqStopChannel = stopChannel
}

// NSQRequeue requeues the message with the specified duration
// and stops sending touches.
func (item *Task) NSQRequeue(delay time.Duration) {
	item.nsqStopChannel <- true
	item.NSQMessage.Requeue(delay)
}

// NSQFinishes the message and stops sending touches.
func (item *Task) NSQFinish() {
	item.nsqStopChannel <- true
	item.NSQMessage.Finish()
}

// StartCalled returns true if NSQStart() has been called on this object.
// This method exist for testing purposes.
func (item *Task) StartCalled() bool {
	return item.nsqStartCalled
}

// TickerStopped returns true if either NSQFinish() or NSQRequeue()
// has been called. This method exist for testing purposes.
func (item *Task) TickerStopped() bool {
	return item.tickerStopped
}
