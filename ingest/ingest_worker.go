package ingest

import (
	//"fmt"
	//"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	//"github.com/APTrust/preservation-services/util"
	"github.com/minio/minio-go/v6"
	//"github.com/satori/go.uuid"
	//"io"
	//"os"
	//"path/filepath"
	//"time"
)

type IngestWorker struct {
	Context      *common.Context
	IngestObject *service.IngestObject
	WorkItemId   int
}

// GetS3Object retrieves a tarred bag from a depositor's receiving bucket.
func (i *IngestWorker) GetS3Object() (*minio.Object, error) {
	return i.Context.S3Clients[constants.S3ClientAWS].GetObject(
		i.IngestObject.S3Bucket,
		i.IngestObject.S3Key,
		minio.GetObjectOptions{})
}

func (i *IngestWorker) IngestFileGet(identifier string) (*service.IngestFile, error) {
	ingestFile, err := i.Context.RedisClient.IngestFileGet(i.WorkItemId, identifier)
	if err != nil {
		i.Context.Logger.Errorf(
			"Failed to retrieve IngestFile from redis: WorkItem %d, %s: %s",
			i.WorkItemId, identifier, err.Error())
	}
	return ingestFile, err
}

func (i *IngestWorker) IngestFileSave(ingestFile *service.IngestFile) error {
	err := i.Context.RedisClient.IngestFileSave(i.WorkItemId, ingestFile)
	if err != nil {
		i.Context.Logger.Errorf(
			"Failed to save IngestFile to redis: WorkItem %d, %s: %s",
			i.WorkItemId, ingestFile.Identifier(), err.Error())
	}
	return err
}

func (i *IngestWorker) IngestObjectSave() error {
	err := i.Context.RedisClient.IngestObjectSave(i.WorkItemId, i.IngestObject)
	if err != nil {
		i.Context.Logger.Errorf(
			"Failed to save IngestObject to redis: WorkItem %d, %s: %s",
			i.WorkItemId, i.IngestObject.Identifier(), err.Error())
	}
	return err
}
