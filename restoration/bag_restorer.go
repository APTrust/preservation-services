package restoration

import (
	"archive/tar"
	"fmt"
	"net/url"
	"strconv"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/minio/minio-go/v6"
)

// The restoration process pipes data as follows:
//
// S3 Preservation Bucket -> TarPipeWriter -> Restoration Bucket
//
// The TarPipeWriter writes all files into a single tarball, which
// will include manifests, tag manifests, and tag files.

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
	}
}

func (r *BagRestorer) Run() (fileCount int, errors []*service.ProcessingError) {

	r.tarPipeWriter = NewTarPipeWriter()
	defer r.tarPipeWriter.Finish()

	r.initUploader()
	fileCount, errors = r.restoreAllPreservedFiles()

	// -------------------------------------------------
	// TODO: Create and copy manifests and tag manifests
	// -------------------------------------------------

	return fileCount, errors
}

// initUploader opens a connection to the depositor's S3 restoration bucket
// using the Minio client's PutObject method. The reader from which PutObject
// copies data comes from the TarPipeWriter. Anything we write into that pipe
// gets copied to the restoration bucket.
func (r *BagRestorer) initUploader() {
	go func() {
		s3Client := r.Context.S3Clients[constants.StorageProviderAWS]
		_, r.uploadError = s3Client.PutObject(
			r.RestorationObject.RestorationTarget,
			r.RestorationObject.Identifier,
			r.tarPipeWriter.GetReader(),
			-1,
			minio.PutObjectOptions{},
		)
	}()
}

// restoreAllPreservedFiles restores all files from the preservation bucket
// to the restoration bucket in the form of a tar archive.
func (r *BagRestorer) restoreAllPreservedFiles() (fileCount int, errors []*service.ProcessingError) {
	fileCount = 1
	hasMore := true
	pageNumber := 1
	for hasMore {
		files, err := r.GetBatchOfFiles(r.RestorationObject.Identifier, pageNumber)
		if err != nil {
			errors = append(errors, r.Error(r.RestorationObject.Identifier, err, false))
			return fileCount, errors
		}
		for _, gf := range files {
			err = r.AddToTarFile(gf)
			if err != nil {
				errors = append(errors, r.Error(gf.Identifier, err, true))
				return fileCount, errors
			}
			fileCount++
			hasMore = len(files) == BatchSize
		}
	}
	r.RestorationObject.AllFilesRestored = true
	return fileCount, errors
}

// BestRestorationSource returns the best preservation bucket from which
// to restore a file. We generally want to restore from S3 over Glacier,
// and US East over other regions. We only need to figure this out once,
// since all of an object's files will be stored in the same preservation
// bucket or buckets.
func (r *BagRestorer) BestRestorationSource(gf *registry.GenericFile) (bestSource *common.PerservationBucket, err error) {
	if r.bestRestorationSource != nil {
		return r.bestRestorationSource, nil
	}
	priority := DefaultPriority
	for _, storageRecord := range gf.StorageRecords {
		for _, preservationBucket := range r.Context.Config.PerservationBuckets {
			if preservationBucket.HostsURL(storageRecord.URL) && preservationBucket.RestorePriority < priority {
				bestSource = preservationBucket
				priority = preservationBucket.RestorePriority
			}
		}
	}
	if priority == DefaultPriority {
		err = fmt.Errorf("Could not find any suitable restoration source for %s. (%d preservation URLS, %d PerservationBuckets", gf.Identifier, len(gf.StorageRecords), len(r.Context.Config.PerservationBuckets))
	}
	return bestSource, err
}

// GetBatchOfFiles returns a batch of GenericFile records from Pharos.
func (r *BagRestorer) GetBatchOfFiles(objectIdentifier string, pageNumber int) (genericFiles []*registry.GenericFile, err error) {
	params := url.Values{}
	params.Set("intellectual_object_identifier", objectIdentifier)
	params.Set("page", strconv.Itoa(pageNumber))
	params.Set("per_page", strconv.Itoa(BatchSize))
	params.Set("sort", "name")
	params.Set("state", "A")
	resp := r.Context.PharosClient.GenericFileList(params)
	return resp.GenericFiles(), resp.Error
}

func (r *BagRestorer) GetTarHeader(gf *registry.GenericFile) *tar.Header {
	return &tar.Header{
		Name:     gf.PathInBag(),
		Size:     gf.Size,
		Typeflag: tar.TypeReg,
		Mode:     int64(0755),
		ModTime:  gf.FileModified,
	}
}

// AddToTarFile adds a GenericFile to the TarPipeWriter. The contents
// go through the TarPipeWriter to restoration bucket.
func (r *BagRestorer) AddToTarFile(gf *registry.GenericFile) (err error) {
	b, err := r.BestRestorationSource(gf)
	if err != nil {
		return err
	}
	client := r.Context.S3Clients[b.Provider]
	obj, err := client.GetObject(b.Bucket, gf.UUID(), minio.GetObjectOptions{})
	if err != nil {
		return err
	}

	// Add header and file data to tarPipeWriter
	tarHeader := r.GetTarHeader(gf)
	return r.tarPipeWriter.AddFile(tarHeader, obj)
}
