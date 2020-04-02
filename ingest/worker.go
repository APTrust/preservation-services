package ingest

import (
	"fmt"

	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

type Worker struct {
	Context      *common.Context
	IngestObject *service.IngestObject
	WorkItemID   int
}

func (w *Worker) IngestFileGet(identifier string) (*service.IngestFile, error) {
	ingestFile, err := w.Context.RedisClient.IngestFileGet(w.WorkItemID, identifier)
	if err != nil {
		w.Context.Logger.Errorf(
			"Failed to retrieve IngestFile from redis: WorkItem %d, %s: %s",
			w.WorkItemID, identifier, err.Error())
	}
	return ingestFile, err
}

func (w *Worker) IngestFileSave(ingestFile *service.IngestFile) error {
	err := w.Context.RedisClient.IngestFileSave(w.WorkItemID, ingestFile)
	if err != nil {
		w.Context.Logger.Errorf(
			"Failed to save IngestFile to redis: WorkItem %d, %s: %s",
			w.WorkItemID, ingestFile.Identifier(), err.Error())
	}
	return err
}

func (w *Worker) IngestObjectSave() error {
	err := w.Context.RedisClient.IngestObjectSave(w.WorkItemID, w.IngestObject)
	if err != nil {
		w.Context.Logger.Errorf(
			"Failed to save IngestObject to redis: WorkItem %d, %s: %s",
			w.WorkItemID, w.IngestObject.Identifier(), err.Error())
	}
	return err
}

func (w *Worker) S3KeyFor(ingestFile *service.IngestFile) string {
	return fmt.Sprintf("%d/%s", w.WorkItemID, ingestFile.UUID)
}

func (w *Worker) Error(identifier string, err error, isFatal bool) *service.ProcessingError {
	return service.NewProcessingError(
		w.WorkItemID,
		identifier,
		err.Error(),
		isFatal,
	)
}
