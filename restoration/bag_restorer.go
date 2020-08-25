package restoration

import (
	"archive/tar"
	"fmt"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
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

var manifestTypes = []string{
	constants.FileTypeManifest,
	constants.FileTypeTagManifest,
}

var bagitTxt = `BagIt-Version: 1.0
Tag-File-Character-Encoding: UTF-8
`

// BagRestorer restores an IntellectualObject in BagIt format to the
// depositor's restoration bucket.
type BagRestorer struct {
	Base
	tarPipeWriter         *TarPipeWriter
	bestRestorationSource *common.PerservationBucket
	bytesWritten          int64
	uploadError           error
	wg                    sync.WaitGroup
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

	r.DeleteStaleManifests()

	r.tarPipeWriter = NewTarPipeWriter()

	r.initUploader()

	err := r.AddBagItFile()
	if err != nil {
		errors = append(errors, r.Error(r.RestorationObject.Identifier, err, true))
		return fileCount, errors
	}

	fileCount, errors = r.restoreAllPreservedFiles()

	manifestsAdded, procErr := r.AddManifests()
	if procErr != nil {
		errors = append(errors, procErr)
		return fileCount, errors
	}
	fileCount += manifestsAdded
	fileCount++ // For bagit.txt

	r.wg.Wait()

	fmt.Println("Bytes Written:", r.bytesWritten)
	fmt.Println("UploadError:", r.uploadError)

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
	r.wg.Add(1)
	go func() {
		s3Client := r.Context.S3Clients[constants.StorageProviderAWS]
		//s3Client.TraceOn(nil)
		r.bytesWritten, r.uploadError = s3Client.PutObject(
			r.RestorationObject.RestorationTarget,
			r.RestorationObject.Identifier+".tar",
			r.tarPipeWriter.GetReader(),
			-1,
			minio.PutObjectOptions{
				//PartSize: 1000000,
			},
		)
		fmt.Println("Upload completed")
		r.Context.Logger.Infof("Finished uploading tar file %s", r.RestorationObject.Identifier)
		r.wg.Done()
	}()
	r.Context.Logger.Infof("Initialized uploader for %s going to %s", r.RestorationObject.Identifier, r.RestorationObject.RestorationTarget)
}

// restoreAllPreservedFiles restores all files from the preservation bucket
// to the restoration bucket in the form of a tar archive.
func (r *BagRestorer) restoreAllPreservedFiles() (fileCount int, errors []*service.ProcessingError) {
	fileCount = 0
	hasMore := true
	pageNumber := 1
	for hasMore {
		files, err := r.GetBatchOfFiles(r.RestorationObject.Identifier, pageNumber)
		if err != nil {
			errors = append(errors, r.Error(r.RestorationObject.Identifier, err, false))
			return fileCount, errors
		}
		for _, gf := range files {
			digests, err := r.AddToTarFile(gf)
			if err != nil {
				r.Context.Logger.Errorf("Error adding %s: %v", gf.Identifier, err)
				errors = append(errors, r.Error(gf.Identifier, err, true))
				return fileCount, errors
			} else {
				r.Context.Logger.Infof("Added %s", gf.Identifier)
			}
			err = r.RecordDigests(gf, digests)
			if err != nil {
				r.Context.Logger.Errorf("Error recording digests for %s: %v", gf.Identifier, err)
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

func (r *BagRestorer) RecordDigests(gf *registry.GenericFile, digests map[string]string) error {
	for _, alg := range constants.SupportedManifestAlgorithms {
		digest := digests[alg]
		registryChecksum := gf.GetLatestChecksum(alg)
		if registryChecksum != nil && digest != registryChecksum.Digest {
			return fmt.Errorf("%s digest mismatch for %s. Pharos says %s, S3 file has %s", alg, gf.Identifier, registryChecksum.Digest, digest)
		}
		err := r.AppendDigestToManifest(gf, digest, alg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *BagRestorer) AppendDigestToManifest(gf *registry.GenericFile, digest, algorithm string) error {
	fileType := constants.FileTypeManifest
	if gf.IsTagFile() {
		fileType = constants.FileTypeTagManifest
	}
	manifestPath := r.GetManifestPath(algorithm, fileType)
	if !util.FileExists(path.Dir(manifestPath)) {
		err := os.MkdirAll(path.Dir(manifestPath), 0755)
		if err != nil {
			return err
		}
	}
	file, err := os.OpenFile(manifestPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = fmt.Fprintf(file, "%s  %s\n", digest, gf.PathInBag())
	return err
}

func (r *BagRestorer) GetManifestPath(algorithm, fileType string) string {
	var filename string
	if fileType == constants.FileTypeTagManifest {
		filename = fmt.Sprintf("tagmanifest-%s.txt", algorithm)
	} else {
		filename = fmt.Sprintf("manifest-%s.txt", algorithm)
	}
	return path.Join(r.Context.Config.RestoreDir, strconv.Itoa(r.WorkItemID), filename)
}

func (r *BagRestorer) DeleteStaleManifests() error {
	for _, alg := range constants.SupportedManifestAlgorithms {
		for _, fileType := range manifestTypes {
			manifestFile := r.GetManifestPath(alg, fileType)
			r.Context.Logger.Info("Deleting old manifest file %s", manifestFile)
			os.Remove(manifestFile)
		}
	}
	return nil
}

// BestRestorationSource returns the best preservation bucket from which
// to restore a file. We generally want to restore from S3 over Glacier,
// and US East over other regions. We only need to figure this out once,
// since all of an object's files will be stored in the same preservation
// bucket or buckets.
//
// You must call this before writing anything to the tar file; otherwise,
// the writes will block forever waiting for a PipeReader to read from
// the PipeWriter.
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
	} else {
		//r.Context.Logger.Infof("Restoring %s from %s", r.RestorationObject.Identifier, r.bestRestorationSource.Bucket)
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
	params.Set("include_storage_records", "true")
	resp := r.Context.PharosClient.GenericFileList(params)
	return resp.GenericFiles(), resp.Error
}

func (r *BagRestorer) GetTarHeader(gf *registry.GenericFile) *tar.Header {
	return &tar.Header{
		Name:     gf.PathMinusInstitution(),
		Size:     gf.Size,
		Typeflag: tar.TypeReg,
		Mode:     int64(0755),
		ModTime:  gf.FileModified,
	}
}

// AddBagItFile adds the bagit.txt file to the tar file.
func (r *BagRestorer) AddBagItFile() error {
	// Add header and file data to tarPipeWriter
	objName, err := r.RestorationObject.ObjName()
	if err != nil {
		return err
	}
	tarHeader := &tar.Header{
		Name:     fmt.Sprintf("%s/%s", objName, "bagit.txt"),
		Size:     int64(len(bagitTxt)),
		Typeflag: tar.TypeReg,
		Mode:     int64(0755),
		ModTime:  time.Now().UTC(),
	}

	digests, err := r.tarPipeWriter.AddFile(tarHeader, strings.NewReader(bagitTxt), r.RestorationObject.ManifestAlgorithms())
	if err != nil {
		return err
	}

	gf := &registry.GenericFile{
		IntellectualObjectIdentifier: r.RestorationObject.Identifier,
		Identifier:                   fmt.Sprintf("%s/%s", r.RestorationObject.Identifier, "bagit.txt"),
	}
	for alg, digest := range digests {
		err = r.AppendDigestToManifest(gf, digest, alg)
		if err != nil {
			return err
		}
	}

	return nil
}

// AddManifests adds manifests and tag manifests to the tar file.
func (r *BagRestorer) AddManifests() (fileCount int, error *service.ProcessingError) {

	// This is the last thing we write, so finish here...
	defer r.tarPipeWriter.Finish()

	objName, err := r.RestorationObject.ObjName()
	if err != nil {
		return fileCount, r.Error(r.RestorationObject.Identifier, err, true)
	}
	for _, alg := range r.RestorationObject.ManifestAlgorithms() {
		for _, fileType := range manifestTypes {
			manifestFile := r.GetManifestPath(alg, fileType)
			manifestName := path.Base(manifestFile)

			r.Context.Logger.Info("Adding %s from %s", manifestName, manifestFile)
			fmt.Printf("Adding %s from %s \n", manifestName, manifestFile)

			fileInfo, err := os.Stat(manifestFile)
			if err != nil {
				return fileCount, r.Error(manifestName, err, true)
			}

			file, err := os.Open(manifestFile)
			if err != nil {
				return fileCount, r.Error(manifestName, err, true)
			}
			defer file.Close()

			tarHeader := &tar.Header{
				Name:     fmt.Sprintf("%s/%s", objName, manifestName),
				Size:     fileInfo.Size(),
				Typeflag: tar.TypeReg,
				Mode:     int64(0755),
				ModTime:  fileInfo.ModTime(),
			}
			// Note that we do not want to calculate digests on manifests.
			// BagIt spec says not to do that, and if we did, we'd be writing
			// into the manifest files on disk as we're pushing those files
			// into the tarball. That will result in data corruption.
			_, err = r.tarPipeWriter.AddFile(tarHeader, file, []string{})
			if err != nil {
				return fileCount, r.Error(manifestName, err, true)
			}
			fileCount++
		}
	}
	return fileCount, nil
}

// AddToTarFile adds a GenericFile to the TarPipeWriter. The contents
// go through the TarPipeWriter to restoration bucket.
func (r *BagRestorer) AddToTarFile(gf *registry.GenericFile) (digests map[string]string, err error) {
	digests = make(map[string]string)
	b, err := r.BestRestorationSource(gf)
	if err != nil {
		return digests, err
	}
	client := r.Context.S3Clients[b.Provider]
	obj, err := client.GetObject(b.Bucket, gf.UUID(), minio.GetObjectOptions{})
	if err != nil {
		return digests, err
	}
	defer obj.Close()

	// Add header and file data to tarPipeWriter
	tarHeader := r.GetTarHeader(gf)
	return r.tarPipeWriter.AddFile(tarHeader, obj, r.RestorationObject.ManifestAlgorithms())
}
