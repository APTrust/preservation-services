package workers

import (
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
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

	// OperationResult describes the result of this worker's work.
	OperationResult *service.OperationResult

	// WorkItem is the Pharos WorkItem that describes the bag, object,
	// of file the worker is working on.
	WorkItem *registry.WorkItem
}

// IngestBase contains the fundamental structures common to all workers.
type IngestBase struct {

	// Context contains info about the context in which the worker is
	// operation, including connections to NSQ, Redis, Pharos, and S3.
	Context *common.Context

	// ItemsInProcess keeps track of WorkItem ids that the worker is
	// currently processing. We need to do this because NSQ does not
	// dedupe messages, so the worker must.
	ItemsInProcess *service.RingList

	// NSQTopic is the name of the NSQ topic to which this worker should
	// subscribe to receive its tasks. The topic names are listed in
	// constants.
	NSQTopic string

	// PreProcessChannel runs checks to ensure that IngestItem should be
	// processed. Since NSQ does not de-dupe messages, the workers must
	// do this themselves.
	PreProcessChannel chan *IngestItem

	// ProcessChannel is where the work actually happens: validation,
	// storage, recording, etc., depending on the worker's responsibility.
	ProcessChannel chan *IngestItem

	// PostProcessChannel is for updating Pharos and NSQ on the status
	// of work. Successfully completed tasks are passed on to the next
	// NSQ topic. Unsuccessful tasks are requeued or sent straight to
	// the cleanup topic. The WorkItem is updated in Pharos with info
	// about its current state and stage.
	PostProcessChannel chan *IngestItem
}

// NewIngestBase creates a new IngestBase worker. Param context is a
// Context object with connections to S3, Redis, Pharos, and NSQ.
// Param bufSize describes the size of the queue buffers.
func NewIngestBase(_context *common.Context, bufSize int, nsqTopic string) IngestBase {
	return IngestBase{
		Context:            _context,
		NSQTopic:           nsqTopic,
		ItemsInProcess:     service.NewRingList(bufSize),
		PreProcessChannel:  make(chan *IngestItem, bufSize),
		ProcessChannel:     make(chan *IngestItem, bufSize),
		PostProcessChannel: make(chan *IngestItem, bufSize),
	}
}
