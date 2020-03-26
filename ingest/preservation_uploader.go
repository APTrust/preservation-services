package ingest

import (
	//"fmt"
	//"net/url"
	//"time"

	//"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	//"github.com/APTrust/preservation-services/util"
)

type PreservationUploader struct {
	Worker
}

func NewPreservationUploader(context *common.Context, workItemID int, ingestObject *service.IngestObject) *PreservationUploader {
	return &PreservationUploader{
		Worker: Worker{
			Context:      context,
			IngestObject: ingestObject,
			WorkItemID:   workItemID,
		},
	}
}

func (uploader *PreservationUploader) getUploadFunction() func(*service.IngestFile) error {
	return func(ingestFile *service.IngestFile) error {
		// START HERE
		return nil
	}
}
