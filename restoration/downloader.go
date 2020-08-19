package restoration

import (
	"fmt"
	"net/url"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/minio/minio-go/v6"
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
	if d.RestorationObject.RestorationType == constants.RestorationTypeFile {
		return d.downloadOne()
	}
	return d.downloadAll()
}

func (d *Downloader) downloadOne() (fileCount int, errors []*service.ProcessingError) {
	identifier := d.RestorationObject.Identifier
	resp := d.Context.PharosClient.GenericFileGet(identifier)
	if resp.Error != nil {
		errors = append(errors, d.Error(identifier, resp.Error, false))
		return 0, errors
	}
	gf := resp.GenericFile()
	err := d.Download(gf)
	if err != nil {
		errors = append(errors, d.Error(identifier, err, false))
		return 0, errors
	}
	return 1, errors
}

func (d *Downloader) downloadAll() (fileCount int, errors []*service.ProcessingError) {
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
			err = d.Download(gf)
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

func (d *Downloader) Download(gf *registry.GenericFile) (err error) {
	b, err := d.BestRestorationSource(gf)
	if err != nil {
		return err
	}
	localPath := fmt.Sprintf("%s/%s", d.RestorationObject.DownloadDir, gf.Identifier)
	err = os.MkdirAll(path.Dir(localPath), 0755)
	if err != nil {
		return err
	}
	client := d.Context.S3Clients[b.Provider]
	for i := 0; i < 3; i++ {
		err = client.FGetObject(b.Bucket, gf.UUID(), localPath, minio.GetObjectOptions{})
		if err == nil {
			d.Context.Logger.Infof("Downloaded %s to %s", gf.UUID(), localPath)
			break
		}
		time.Sleep(1 * time.Second)
	}
	return err
}
