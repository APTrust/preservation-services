package ingest

import (
	"fmt"

	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

type BaseConstructor func(*common.Context, int, *service.IngestObject) Runnable

type Runnable interface {
	Run() (int, []*service.ProcessingError)
	GetIngestObject() *service.IngestObject
}

// Base is the base type for workers in the ingest namespace.
type Base struct {
	Context      *common.Context
	IngestObject *service.IngestObject
	WorkItemID   int
}

// GetIngestObject resturns this struct's IngestObject. This satisfies part
// of the Runnable interface.
func (b *Base) GetIngestObject() *service.IngestObject {
	return b.IngestObject
}

// IngestFileGet returns an IngestFile record from Redis.
func (b *Base) IngestFileGet(identifier string) (*service.IngestFile, error) {
	ingestFile, err := b.Context.RedisClient.IngestFileGet(b.WorkItemID, identifier)
	if err != nil {
		b.Context.Logger.Errorf(
			"Failed to retrieve IngestFile from redis: WorkItem %d, %s: %s",
			b.WorkItemID, identifier, err.Error())
	}
	return ingestFile, err
}

// IngestFileSave saves an IngestFile to Redis.
func (b *Base) IngestFileSave(ingestFile *service.IngestFile) error {
	err := b.Context.RedisClient.IngestFileSave(b.WorkItemID, ingestFile)
	if err != nil {
		b.Context.Logger.Errorf(
			"Failed to save IngestFile to redis: WorkItem %d, %s: %s",
			b.WorkItemID, ingestFile.Identifier(), err.Error())
	}
	return err
}

// IngestObjectSave saves an IngestObject record to Redis.
func (b *Base) IngestObjectSave() error {
	err := b.Context.RedisClient.IngestObjectSave(b.WorkItemID, b.IngestObject)
	if err != nil {
		b.Context.Logger.Errorf(
			"Failed to save IngestObject to redis: WorkItem %d, %s: %s",
			b.WorkItemID, b.IngestObject.Identifier(), err.Error())
	} else {
		b.Context.Logger.Errorf(
			"Saved IngestObject to redis: WorkItem %d, %s",
			b.WorkItemID, b.IngestObject.Identifier())
	}
	return err
}

// S3KeyFor returns the S3 key for an ingest file in the staging bucket.
// Note that the staging bucket uses UUID keys, not file identifiers.
func (b *Base) S3KeyFor(ingestFile *service.IngestFile) string {
	return fmt.Sprintf("%d/%s", b.WorkItemID, ingestFile.UUID)
}

// Error returns a ProcessingError describing something that went wrong
// during the ingest process for this object. Identifier can be either
// an IntellectualObect identifier, a GenericFile identifier, or in rare
// cases a WorkItem ID. Since each has a different format, you can discern
// the identifier type by looking at it.
func (b *Base) Error(identifier string, err error, isFatal bool) *service.ProcessingError {
	return service.NewProcessingError(
		b.WorkItemID,
		identifier,
		err.Error(),
		isFatal,
	)
}
