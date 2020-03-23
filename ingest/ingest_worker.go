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

func (i *IngestWorker) IngestFileGet(gfIdentifier string) (*service.IngestFile, error) {
	return i.Context.RedisClient.IngestFileGet(i.WorkItemId, gfIdentifier)
}

func (i *IngestWorker) IngestFileSave(ingestFile *service.IngestFile) error {
	return i.Context.RedisClient.IngestFileSave(i.WorkItemId, ingestFile)
}
