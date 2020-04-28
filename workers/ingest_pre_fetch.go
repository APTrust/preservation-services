package workers

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	//"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	//"github.com/nsqio/go-nsq"
)

// constants.NSQIngestPreFetchTopic

type IngestPreFetch struct {
	IngestBase
}

func NewIngestPreFetch(bufSize int) *IngestPreFetch {
	worker := &IngestPreFetch{
		IngestBase: NewIngestBase(
			common.NewContext(),
			createMetadataGatherer,
			bufSize,
			constants.IngestPreFetch,
		),
	}

	// TODO: Set up go routines based on _context.Config.Ingest*Workers setting

	return worker
}

func createMetadataGatherer(context *common.Context, workItemID int, ingestObject *service.IngestObject) ingest.Runnable {
	return ingest.NewMetadataGatherer(context, workItemID, ingestObject)
}
