package restoration

import (
	// "fmt"
	// "net/url"
	// "os"
	// "path"
	// "strconv"
	// "time"

	"github.com/APTrust/preservation-services/models/common"
	// "github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	// "github.com/minio/minio-go/v6"
)

// FileRestorer restores individual files to a depositor's restoration bucket.
type FileRestorer struct {
	Base
}

// NewFileRestorer creates a new FileRestorer to copy files from S3
// to local disk for packaging.
func NewFileRestorer(context *common.Context, workItemID int, restorationObject *service.RestorationObject) *FileRestorer {
	return &FileRestorer{
		Base: Base{
			Context:           context,
			RestorationObject: restorationObject,
			WorkItemID:        workItemID,
		},
	}
}

func (d *FileRestorer) Run() (fileCount int, errors []*service.ProcessingError) {

	if len(errors) == 0 {
		fileCount = 1
		d.RestorationObject.AllFilesRestored = true
	}
	return fileCount, errors
}
