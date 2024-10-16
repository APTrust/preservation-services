package ingest

import (
	"fmt"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

// PreservationVerifier verifies that all files were copied correctly
// to preservation storage.
type PreservationVerifier struct {
	Base
}

// NewPreservationVerifier returns a new PreservationVerifier.
func NewPreservationVerifier(context *common.Context, workItemID int64, ingestObject *service.IngestObject) *PreservationVerifier {
	return &PreservationVerifier{
		Base: Base{
			Context:      context,
			IngestObject: ingestObject,
			WorkItemID:   workItemID,
		},
	}
}

// Run verifies that all files were copied to preservation storage.
// Note that this relies on the StorageRecords attached to each IngestFile.
// It does not attempt to ensure that the StorageRecords themselves are
// valid for the StorageOption. The PerservationUploader is responsible for
// that. This just verifies that the PreservationUploader did what it said
// it did.
func (v *PreservationVerifier) Run() (int, []*service.ProcessingError) {
	verifyFn := v.getVerifyFunction()
	options := service.IngestFileApplyOptions{
		MaxErrors:   30,
		MaxRetries:  3,
		RetryMs:     1000,
		SaveChanges: true,
		WorkItemID:  v.WorkItemID,
	}
	return v.Context.RedisClient.IngestFilesApply(verifyFn, options)
}

func (v *PreservationVerifier) getVerifyFunction() func(*service.IngestFile) (errors []*service.ProcessingError) {
	return func(ingestFile *service.IngestFile) (errors []*service.ProcessingError) {
		for _, record := range ingestFile.StorageRecords {
			v.Context.Logger.Infof("Verifying %s (%s) in %s %s", ingestFile.Identifier(), ingestFile.UUID, record.Provider, record.Bucket)
			objInfo, err := v.Context.S3StatObject(
				record.Provider,
				record.Bucket,
				ingestFile.UUID,
			)
			// Should check err type -> "no such key" should be fatal
			if err != nil {
				v.Context.Logger.Errorf("Error for %s (%s) in %s %s: %v", ingestFile.Identifier(), ingestFile.UUID, record.Provider, record.Bucket, err)
				errors = append(errors, v.Error(ingestFile.Identifier(), err, false))
			} else {
				record.ETag = strings.Replace(objInfo.ETag, "\"", "", -1)
				record.Size = objInfo.Size
				if record.Size == ingestFile.Size {
					v.Context.Logger.Infof("Verified %s (%s) is in %s %s with size %d and etag %s", ingestFile.Identifier(), ingestFile.UUID, record.Provider, record.Bucket, record.Size, record.ETag)
					record.VerifiedAt = time.Now().UTC()
				} else {
					err = fmt.Errorf("Preservation size %d does not match recorded file size %d", record.Size, ingestFile.Size)
					v.Context.Logger.Errorf("Error for %s (%s) in %s %s: %v", ingestFile.Identifier(), ingestFile.UUID, record.Provider, record.Bucket, err)
					record.Error = err.Error()
					errors = append(errors, v.Error(ingestFile.Identifier(), err, false))
				}
			}
		}
		return errors
	}
}
