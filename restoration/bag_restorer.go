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

const batchSize = 100
const defaultPriority = 10000

// Contents of the bagit.txt file. We have to write this into
// every restored bag.
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

// Run restores the entire bag to the depositor's restoration bucket.
func (r *BagRestorer) Run() (fileCount int, errors []*service.ProcessingError) {

	// Yes, we do this at the beginning and end of each run.
	// If bag restoration worker was shut down on an interrupt signal,
	// old manifests might persist. Those will cause an invalid bag
	// on the next restoration attempt.
	r.DeleteStaleManifests()
	defer r.DeleteStaleManifests()

	r.Context.Logger.Infof("Bag %s has profile %s (%s)", r.RestorationObject.Identifier, r.RestorationObject.BagItProfileIdentifier, r.RestorationObject.BagItProfile())

	r.tarPipeWriter = NewTarPipeWriter()

	r.initUploader()

	err := r.AddBagItFile()
	if err != nil {
		errors = append(errors, r.Error(r.RestorationObject.Identifier, err, true))
		return fileCount, errors
	}

	// Restore payload files and preserved tag files.
	fileCount, errors = r.restoreAllPreservedFiles()

	// Add payload manifests before tag manifests, because we need to
	// calculate checksums on the payload manifests.
	manifestsAdded, procErr := r.AddManifests(constants.FileTypeManifest)
	if procErr != nil {
		errors = append(errors, procErr)
		return fileCount, errors
	}
	fileCount += manifestsAdded

	// Lastly, add tag manifests, which include checksums for tag files
	// and for payload manifests.
	tagManifestsAdded, procErr := r.AddManifests(constants.FileTypeTagManifest)
	if procErr != nil {
		errors = append(errors, procErr)
		return fileCount, errors
	}
	fileCount += tagManifestsAdded

	fileCount++ // For bagit.txt

	// Close the PipeWriter, or the PipeReader will hang forever.
	r.tarPipeWriter.Finish()

	r.wg.Wait()

	if len(errors) == 0 {
		r.RestorationObject.AllFilesRestored = true
		r.RestorationObject.URL = fmt.Sprintf("%s%s/%s", constants.AWSBucketPrefix, r.RestorationObject.RestorationTarget, r.RestorationObject.Identifier)
	}

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
			hasMore = len(files) == batchSize
		}
	}
	return fileCount, errors
}

// RecordDigests appends file checksums to manifests and tag manifests.
// These files are stored locally on disk and added to the tarred bag
// at the end of the bagging process. They will always be the last items
// written into the tar file. Manifests typically range from 1-20 kilobytes.
// In bags with hundreds of thousands of files, manifests can be several
// megabytes, but these are rare.
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

// AppendDigestToManifest adds the given digest (checksum) for the
// specified file to the end of a manifest.
func (r *BagRestorer) AppendDigestToManifest(gf *registry.GenericFile, digest, algorithm string) error {
	// Payload digests go into manifest.
	// Digests of tag files and payload manifests go into tag manifests.
	fileType := constants.FileTypeManifest
	if gf.IsTagFile() || util.LooksLikeManifest(gf.PathInBag()) {
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

// GetManifestPath returns the path to a manifest or tag manifest on
// local disk.
func (r *BagRestorer) GetManifestPath(algorithm, fileType string) string {
	var filename string
	if fileType == constants.FileTypeTagManifest {
		filename = fmt.Sprintf("tagmanifest-%s.txt", algorithm)
	} else {
		filename = fmt.Sprintf("manifest-%s.txt", algorithm)
	}
	return path.Join(r.Context.Config.RestoreDir, strconv.Itoa(r.WorkItemID), filename)
}

// DeleteStaleManifests deletes any manifests and tag manifests left over
// from prior attempts to restore this bag.
func (r *BagRestorer) DeleteStaleManifests() error {
	for _, alg := range constants.SupportedManifestAlgorithms {
		for _, fileType := range constants.ManifestTypes {
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
	priority := defaultPriority
	for _, storageRecord := range gf.StorageRecords {
		for _, preservationBucket := range r.Context.Config.PerservationBuckets {
			if preservationBucket.HostsURL(storageRecord.URL) && preservationBucket.RestorePriority < priority {
				bestSource = preservationBucket
				priority = preservationBucket.RestorePriority
			}
		}
	}
	if priority == defaultPriority {
		err = fmt.Errorf("Could not find any suitable restoration source for %s. (%d preservation URLS, %d PerservationBuckets", gf.Identifier, len(gf.StorageRecords), len(r.Context.Config.PerservationBuckets))
	} else {
		r.Context.Logger.Infof("Restoring %s from %s", r.RestorationObject.Identifier, bestSource.Bucket)
	}
	return bestSource, err
}

// GetBatchOfFiles returns a batch of GenericFile records from Pharos.
func (r *BagRestorer) GetBatchOfFiles(objectIdentifier string, pageNumber int) (genericFiles []*registry.GenericFile, err error) {
	params := url.Values{}
	params.Set("intellectual_object_identifier", objectIdentifier)
	params.Set("page", strconv.Itoa(pageNumber))
	params.Set("per_page", strconv.Itoa(batchSize))
	params.Set("sort", "name")
	params.Set("state", "A")
	params.Set("include_storage_records", "true")
	resp := r.Context.PharosClient.GenericFileList(params)
	return resp.GenericFiles(), resp.Error
}

// GetTarHeader returns a tar header for the specified GenericFile.
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
func (r *BagRestorer) AddManifests(manifestType string) (fileCount int, error *service.ProcessingError) {
	for _, alg := range r.RestorationObject.ManifestAlgorithms() {
		manifestFile := r.GetManifestPath(alg, manifestType)
		err := r._addManifest(manifestFile, manifestType)
		if err != nil {
			return fileCount, r.Error(manifestFile, err, true)
		}
		fileCount++
	}
	return fileCount, nil
}

func (r *BagRestorer) _addManifest(manifestFile, manifestType string) error {
	objName, err := r.RestorationObject.ObjName()
	if err != nil {
		return err
	}
	manifestName := path.Base(manifestFile)
	r.Context.Logger.Info("Adding %s from %s", manifestName, manifestFile)
	fileInfo, err := os.Stat(manifestFile)
	if err != nil {
		return err
	}
	file, err := os.Open(manifestFile)
	if err != nil {
		return err
	}
	defer file.Close()

	tarHeader := &tar.Header{
		Name:     fmt.Sprintf("%s/%s", objName, manifestName),
		Size:     fileInfo.Size(),
		Typeflag: tar.TypeReg,
		Mode:     int64(0755),
		ModTime:  fileInfo.ModTime(),
	}

	// Calculate digests on manifests, but not on tag manifests.
	// Tag manifest files will contain digests of manifest files.
	algs := make([]string, 0)
	if manifestType == constants.FileTypeManifest {
		algs = r.RestorationObject.ManifestAlgorithms()
	}

	digests, err := r.tarPipeWriter.AddFile(tarHeader, file, algs)
	if err != nil {
		return err
	}

	// If this is a payload manifest, add its checksum to the appropriate
	// tag manifests.
	if manifestType == constants.FileTypeManifest {
		gf := &registry.GenericFile{
			IntellectualObjectIdentifier: r.RestorationObject.Identifier,
			Identifier:                   fmt.Sprintf("%s/%s", r.RestorationObject.Identifier, manifestName),
		}
		for alg, digest := range digests {
			err = r.AppendDigestToManifest(gf, digest, alg)
			if err != nil {
				return err
			}
		}
	}

	return nil
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

// IngestObjectGet satisfies Runnable interface. Does nothing because
// we don't work with IngestObjects in this context.
func (r *BagRestorer) IngestObjectGet() *service.IngestObject {
	return nil
}

// IngestObjectSave satisfies Runnable interface. Does nothing because
// we don't work with IngestObjects in this context.
func (r *BagRestorer) IngestObjectSave() error {
	return nil
}
