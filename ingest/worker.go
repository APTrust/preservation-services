package ingest

import (
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
func (i *Worker) GetS3Object() (*minio.Object, error) {
	return i.Context.S3Clients[constants.StorageProviderAWS].GetObject(
		i.IngestObject.S3Bucket,
		i.IngestObject.S3Key,
		minio.GetObjectOptions{})
}

func (i *Worker) IngestFileGet(identifier string) (*service.IngestFile, error) {
	ingestFile, err := i.Context.RedisClient.IngestFileGet(i.WorkItemID, identifier)
	if err != nil {
		i.Context.Logger.Errorf(
			"Failed to retrieve IngestFile from redis: WorkItem %d, %s: %s",
			i.WorkItemID, identifier, err.Error())
	}
	return ingestFile, err
}

func (i *Worker) IngestFileSave(ingestFile *service.IngestFile) error {
	err := i.Context.RedisClient.IngestFileSave(i.WorkItemID, ingestFile)
	if err != nil {
		i.Context.Logger.Errorf(
			"Failed to save IngestFile to redis: WorkItem %d, %s: %s",
			i.WorkItemID, ingestFile.Identifier(), err.Error())
	}
	return err
}

func (i *Worker) IngestObjectSave() error {
	err := i.Context.RedisClient.IngestObjectSave(i.WorkItemID, i.IngestObject)
	if err != nil {
		i.Context.Logger.Errorf(
			"Failed to save IngestObject to redis: WorkItem %d, %s: %s",
			i.WorkItemID, i.IngestObject.Identifier(), err.Error())
	}
	return err
}
