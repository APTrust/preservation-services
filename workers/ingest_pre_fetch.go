package workers

import (
	//"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	//"github.com/APTrust/preservation-services/models/registry"
	//"github.com/APTrust/preservation-services/models/service"
	//"github.com/nsqio/go-nsq"
)

// constants.NSQIngestPreFetchTopic

type IngestPreFetch struct {
	IngestBase
}

func NewIngestPreFetch(_context *common.Context, bufSize int, nsqTopic string) *IngestPreFetch {
	worker := &IngestPreFetch{
		IngestBase: NewIngestBase(_context, bufSize, nsqTopic),
	}

	// TODO: Set up go routines based on _context.Config.Ingest*Workers setting

	return worker
}
