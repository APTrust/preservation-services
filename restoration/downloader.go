package restoration

import (
	"net/url"
	"strconv"

	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
)

const BatchSize = 100

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
	fileCount = 1
	objIdentifier := d.RestorationObject.ObjIdentifier
	hasMore := true
	pageNumber := 1
	for hasMore {
		files, err := d.GetBatchOfFiles(objIdentifier, pageNumber)
		if err != nil {
			errors = append(errors, d.Error(objIdentifier, err, false))
			break
		}
		hasMore = len(files) == BatchSize
		for _, gf := range files {
			bucket := ""
			key := ""
			_, err = d.Download(bucket, key)
			if err != nil {
				errors = append(errors, d.Error(gf.Identifier, err, false))
				if len(errors) >= 30 {
					break
				}
			} else {
				fileCount++
			}
		}
	}
	if len(errors) == 0 {
		d.RestorationObject.AllFilesDownloaded = true
	}
	return fileCount, errors
}

// GetBatchOfFiles returns a batch of GenericFile records from Pharos.
func (d *Downloader) GetBatchOfFiles(objectIdentifier string, pageNumber int) (genericFiles []*registry.GenericFile, err error) {
	params := url.Values{}
	params.Set("intellectual_object_identifier", objectIdentifier)
	params.Set("page", strconv.Itoa(pageNumber))
	params.Set("per_page", strconv.Itoa(BatchSize))
	params.Set("sort", "name")
	params.Set("state", "A")
	resp := d.Context.PharosClient.GenericFileList(params)
	return resp.GenericFiles(), resp.Error
}

func (d *Downloader) Download(bucket, key string) (filepath string, err error) {
	return filepath, err
}
