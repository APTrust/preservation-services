package restoration

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
)

const BatchSize = 100
const DefaultPriority = 10000

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
	objIdentifier := d.RestorationObject.Identifier
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
			restorationSource, err := d.BestRestorationSource(gf)
			if err != nil {
				// Fatal error if we can't find restoration source
				errors = append(errors, d.Error(gf.Identifier, err, true))
				break
			}
			_, err = d.Download(restorationSource, gf.UUID())
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

// BestRestorationSource returns the best preservation bucket from which
// to restore a file. We generally want to restore from S3 over Glacier,
// and US East over other regions.
func (d *Downloader) BestRestorationSource(gf *registry.GenericFile) (bestSource *common.PerservationBucket, err error) {
	priority := DefaultPriority
	for _, storageRecord := range gf.StorageRecords {
		for _, preservationBucket := range d.Context.Config.PerservationBuckets {
			if preservationBucket.HostsURL(storageRecord.URL) && preservationBucket.RestorePriority < priority {
				bestSource = preservationBucket
				priority = preservationBucket.RestorePriority
			}
		}
	}
	if priority == DefaultPriority {
		err = fmt.Errorf("Could not find any suitable restoration source for %s. (%d preservation URLS, %d PerservationBuckets", gf.Identifier, len(gf.StorageRecords), len(d.Context.Config.PerservationBuckets))
	}
	return bestSource, err
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

func (d *Downloader) Download(preservationBucket *common.PerservationBucket, key string) (filepath string, err error) {
	return filepath, err
}
