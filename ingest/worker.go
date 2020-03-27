package ingest

import (
	"fmt"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/minio/minio-go/v6"
)

type Worker struct {
	Context      *common.Context
	IngestObject *service.IngestObject
	WorkItemID   int
}

// GetS3Object retrieves a tarred bag from a depositor's receiving bucket.
func (w *Worker) GetS3Object() (*minio.Object, error) {
	return w.Context.S3Clients[constants.StorageProviderAWS].GetObject(
		w.IngestObject.S3Bucket,
		w.IngestObject.S3Key,
		minio.GetObjectOptions{})
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
