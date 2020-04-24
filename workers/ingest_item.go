package workers

import (
	"time"

	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/nsqio/go-nsq"
)

// IngestItem encapsulates everything that a worker will need to
// pass from one channel to the next during procesing.
type IngestItem struct {

	// NSQMessage is the NSQ message the worker is processing.
	NSQMessage *nsq.Message

	// Processor is handles whatever phase of the ingest process
	// this worker is responsible for (validation, storage, recording, etc.)
	Processor *ingest.Base

	// WorkResult describes the result of this worker's work.
	WorkResult *service.WorkResult

	// WorkItem is the Pharos WorkItem that describes the bag, object,
	// of file the worker is working on.
	WorkItem *registry.WorkItem

	nsqStopChannel chan bool
}

// NSQStart creates a timer that touches the NSQ message
// every two minutes while the WorkItem is in process. We need this
// because operations like calculating checksums on a 200GB file
// cannot pause to touch the NSQ message before it times out.
func (item *IngestItem) NSQStart() {
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
				return
			}
		}
	}()
	item.nsqStopChannel = stopChannel
}

// NSQRequeue requeues the message with the specified duration
// and stops sending touches.
func (item *IngestItem) NSQRequeue(delay time.Duration) {
	item.nsqStopChannel <- true
	item.NSQMessage.Requeue(delay)
}

// NSQFinishes the message and stops sending touches.
func (item *IngestItem) NSQFinish(delay time.Duration) {
	item.nsqStopChannel <- true
	item.NSQMessage.Finish()
}
