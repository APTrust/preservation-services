package workers

import (
	"time"

	"github.com/APTrust/preservation-services/deletion"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/nsqio/go-nsq"
)

// *********************************************************************
// TODO: Factor out methods common to this and IngestItem
// *********************************************************************

// DeletionItem encapsulates everything that a worker will need to
// pass from one channel to the next during procesing.
type DeletionItem struct {

	// Manager does the work of deleting files from S3/Glacier.
	Manager *deletion.Manager

	// NSQMessage is the NSQ message the worker is processing.
	NSQMessage *nsq.Message

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
func (item *DeletionItem) NSQStart() {
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
func (item *DeletionItem) NSQRequeue(delay time.Duration) {
	item.nsqStopChannel <- true
	item.NSQMessage.Requeue(delay)
}

// NSQFinishes the message and stops sending touches.
func (item *DeletionItem) NSQFinish() {
	item.nsqStopChannel <- true
	item.NSQMessage.Finish()
}

// StartCalled returns true if NSQStart() has been called on this object.
// This method exist for testing purposes.
func (item *DeletionItem) StartCalled() bool {
	return item.nsqStartCalled
}

// TickerStopped returns true if either NSQFinish() or NSQRequeue()
// has been called. This method exist for testing purposes.
func (item *DeletionItem) TickerStopped() bool {
	return item.tickerStopped
}
