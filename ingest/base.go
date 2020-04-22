package ingest

import (
	"fmt"

	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

type Base struct {
	Context      *common.Context
	IngestObject *service.IngestObject
	WorkItemID   int
}

func (b *Base) IngestFileGet(identifier string) (*service.IngestFile, error) {
	ingestFile, err := b.Context.RedisClient.IngestFileGet(b.WorkItemID, identifier)
	if err != nil {
		b.Context.Logger.Errorf(
			"Failed to retrieve IngestFile from redis: WorkItem %d, %s: %s",
			b.WorkItemID, identifier, err.Error())
	}
	return ingestFile, err
}

func (b *Base) IngestFileSave(ingestFile *service.IngestFile) error {
	err := b.Context.RedisClient.IngestFileSave(b.WorkItemID, ingestFile)
	if err != nil {
		b.Context.Logger.Errorf(
			"Failed to save IngestFile to redis: WorkItem %d, %s: %s",
			b.WorkItemID, ingestFile.Identifier(), err.Error())
	}
	return err
}

func (b *Base) IngestObjectSave() error {
	err := b.Context.RedisClient.IngestObjectSave(b.WorkItemID, b.IngestObject)
	if err != nil {
		b.Context.Logger.Errorf(
			"Failed to save IngestObject to redis: WorkItem %d, %s: %s",
			b.WorkItemID, b.IngestObject.Identifier(), err.Error())
	}
	return err
}

func (b *Base) S3KeyFor(ingestFile *service.IngestFile) string {
	return fmt.Sprintf("%d/%s", b.WorkItemID, ingestFile.UUID)
}

func (b *Base) Error(identifier string, err error, isFatal bool) *service.ProcessingError {
	return service.NewProcessingError(
		b.WorkItemID,
		identifier,
		err.Error(),
		isFatal,
	)
}
