package restoration

import (
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

// Downloader downloads all files required to restore an IntellectualObject.
type Downloader struct {
	Base
}

// NewDownloader creates a new Downloader to copy files from S3
// to local disk for packaging.
func NewDownloader(context *common.Context, workItemID int, restorationObject *service.RestorationObject) *Downloader {
	return &Downloader{
		Base: Base{
			Context:           context,
			RestorationObject: restorationObject,
			WorkItemID:        workItemID,
		},
	}
}

func (d *Downloader) Run() (fileCount int, errors []*service.ProcessingError) {
	// Loop:
	// Get batch of files from Pharos
	// Download each file
	// Mark RestorationObject.AllFilesDownloaded to true
	return fileCount, errors
}
