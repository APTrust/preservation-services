package restoration

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/minio/minio-go/v6"
)

const BatchSize = 100
const DefaultPriority = 10000

// BagRestorer restores an IntellectualObject in BagIt format to the
// depositor's restoration bucket.
type BagRestorer struct {
	Base
	tarPipeWriter         *TarPipeWriter
	bestRestorationSource *common.PerservationBucket
	uploadError           error
}

// NewBagRestorer creates a new BagRestorer to copy files from S3
// to local disk for packaging.
func NewBagRestorer(context *common.Context, workItemID int, restorationObject *service.RestorationObject) *BagRestorer {
	return &BagRestorer{
		Base: Base{
			Context:           context,
			RestorationObject: restorationObject,
			WorkItemID:        workItemID,
		},
		tarPipeWriter: NewTarPipeWriter(),
	}
}

func (d *BagRestorer) Run() (fileCount int, errors []*service.ProcessingError) {

	bucket := d.RestorationObject.RestorationTarget
	objIdentifier := d.RestorationObject.Identifier

	// Need WaitGroup

	go func() {
		s3Client := d.Context.S3Clients[constants.StorageProviderAWS]
		_, d.uploadError = s3Client.PutObject(
			bucket,
			objIdentifier,
			d.tarPipeWriter.GetReader(),
			-1,
			minio.PutObjectOptions{},
		)
	}()

	fileCount = 1
	hasMore := true
	pageNumber := 1
	for hasMore {
		files, err := d.GetBatchOfFiles(objIdentifier, pageNumber)
		if err != nil {
			errors = append(errors, d.Error(objIdentifier, err, false))
			return fileCount, errors
		}
		for _, gf := range files {
			err = d.AddToTarFile(gf)
			if err != nil {
				errors = append(errors, d.Error(gf.Identifier, err, true))
				return fileCount, errors
			}
			fileCount++
			hasMore = len(files) == BatchSize
		}
	}
	d.RestorationObject.AllFilesRestored = true

	// Need WaitGroup
	// Close tarPipeWriter reader and writer

	return fileCount, errors
}

// BestRestorationSource returns the best preservation bucket from which
// to restore a file. We generally want to restore from S3 over Glacier,
// and US East over other regions. We only need to figure this out once,
// since all of an object's files will be stored in the same preservation
// bucket or buckets.
func (d *BagRestorer) BestRestorationSource(gf *registry.GenericFile) (bestSource *common.PerservationBucket, err error) {
	if d.bestRestorationSource != nil {
		return d.bestRestorationSource, nil
	}
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
func (d *BagRestorer) GetBatchOfFiles(objectIdentifier string, pageNumber int) (genericFiles []*registry.GenericFile, err error) {
	params := url.Values{}
	params.Set("intellectual_object_identifier", objectIdentifier)
	params.Set("page", strconv.Itoa(pageNumber))
	params.Set("per_page", strconv.Itoa(BatchSize))
	params.Set("sort", "name")
	params.Set("state", "A")
	resp := d.Context.PharosClient.GenericFileList(params)
	return resp.GenericFiles(), resp.Error
}

func (d *BagRestorer) AddToTarFile(gf *registry.GenericFile) (err error) {
	// b, err := d.BestRestorationSource(gf)
	// if err != nil {
	// 	return err
	// }
	// client := d.Context.S3Clients[b.Provider]
	// obj, err := client.GetObject(b.Bucket, gf.UUID(), minio.GetObjectOptions{})
	// if err != nil {
	// 	return err
	// }

	// // Construct header
	// // Add header and file data to tarPipeWriter

	return err
}
