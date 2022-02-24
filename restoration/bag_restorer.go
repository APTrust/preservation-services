package restoration

import (
	"archive/tar"
	ctx "context"
	"fmt"
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
	"github.com/minio/minio-go/v7"
)

// The restoration process pipes data as follows:
//
// S3 Preservation Bucket -> TarPipeWriter -> Restoration Bucket
//
// The TarPipeWriter writes all files into a single tarball, which
// will include manifests, tag manifests, and tag files.

const batchSize = 100

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
	bestRestorationSource *common.PreservationBucket
	bytesWritten          int64
	uploadError           error
	wg                    sync.WaitGroup
}

// NewBagRestorer creates a new BagRestorer to copy files from S3
// to local disk for packaging.
func NewBagRestorer(context *common.Context, workItemID int64, restorationObject *service.RestorationObject) *BagRestorer {
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
		r.RestorationObject.URL = fmt.Sprintf("%s%s/%s.tar", constants.AWSBucketPrefix, r.RestorationObject.RestorationTarget, r.RestorationObject.Identifier)
	}

	return fileCount, errors
}

// initUploader opens a connection to the depositor's S3 restoration bucket
// using the Minio client's PutObject method. The reader from which PutObject
// copies data comes from the TarPipeWriter. Anything we write into that pipe
// gets copied to the restoration bucket.
func (r *BagRestorer) initUploader() {
	r.wg.Add(1)

	estimatedObjectSize := float64(r.RestorationObject.ObjectSize) * float64(1.10)
	chunkSize := util.EstimatedChunkSize(estimatedObjectSize)

	r.Context.Logger.Infof("Initializing uploader. "+
		"Object size = %.0f. Chunk size = %d",
		estimatedObjectSize, chunkSize)

	go func() {
		s3Client := r.Context.S3Clients[constants.StorageProviderAWS]
		//s3Client.TraceOn(nil)

		defer func() {
			if rec := recover(); rec != nil {
				r.Context.Logger.Errorf("Uploader panicked. "+
					"Possible large memory allocation. "+
					"Object size = %d. Chunk size = %d",
					estimatedObjectSize, chunkSize)
				r.Context.Logger.Errorf("Panic info: %v", rec)
			}
		}()

		var uploadInfo minio.UploadInfo
		uploadInfo, r.uploadError = s3Client.PutObject(
			ctx.Background(),
			r.RestorationObject.RestorationTarget,
			r.RestorationObject.Identifier+".tar",
			r.tarPipeWriter.GetReader(),
			-1,
			minio.PutObjectOptions{
				PartSize: chunkSize,
			},
		)
		r.bytesWritten = uploadInfo.Size
		r.Context.Logger.Infof("Finished uploading tar file %s", r.RestorationObject.Identifier)
		r.wg.Done()
	}()
	r.Context.Logger.Infof("Initialized uploader for %s going to %s", r.RestorationObject.Identifier, r.RestorationObject.RestorationTarget)
}

// restoreAllPreservedFiles restores all files from the preservation bucket
// to the restoration bucket in the form of a tar archive.
func (r *BagRestorer) restoreAllPreservedFiles() (fileCount int, errors []*service.ProcessingError) {
	fileCount = 0
	pageNumber := 1
	for {
		files, err := GetBatchOfFiles(r.Context, r.RestorationObject.ItemID, pageNumber)
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
		}
		if len(files) == 0 {
			break
		}
		pageNumber++
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
			return fmt.Errorf("%s digest mismatch for %s. Registry says %s, S3 file has %s", alg, gf.Identifier, registryChecksum.Digest, digest)
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
	pathInBag, err := gf.PathInBag()
	if err != nil {
		return err
	}
	if gf.IsTagFile() || util.LooksLikeManifest(pathInBag) {
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
	_, err = fmt.Fprintf(file, "%s  %s\n", digest, pathInBag)
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
	return path.Join(r.Context.Config.RestoreDir, strconv.FormatInt(r.WorkItemID, 10), filename)
}

// DeleteStaleManifests deletes any manifests and tag manifests left over
// from prior attempts to restore this bag.
func (r *BagRestorer) DeleteStaleManifests() error {
	for _, alg := range constants.SupportedManifestAlgorithms {
		for _, fileType := range constants.ManifestTypes {
			manifestFile := r.GetManifestPath(alg, fileType)
			r.Context.Logger.Infof("Deleting old manifest file %s", manifestFile)
			os.Remove(manifestFile)
		}
	}
	return nil
}

// GetTarHeader returns a tar header for the specified GenericFile.
func (r *BagRestorer) GetTarHeader(gf *registry.GenericFile) *tar.Header {
	pathMinusInstitution, _ := gf.PathMinusInstitution()
	modTime := gf.FileModified
	if modTime.IsZero() {
		modTime = time.Now().UTC()
	}
	return &tar.Header{
		Name:     pathMinusInstitution,
		Size:     gf.Size,
		Typeflag: tar.TypeReg,
		Mode:     int64(0755),
		ModTime:  modTime,
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
		Identifier: fmt.Sprintf("%s/%s", r.RestorationObject.Identifier, "bagit.txt"),
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
			Identifier: fmt.Sprintf("%s/%s", r.RestorationObject.Identifier, manifestName),
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
	b, _, err := BestRestorationSource(r.Context, gf)
	if err != nil {
		return digests, err
	}
	r.Context.Logger.Infof("Getting %s from %s with UUID %s", gf.Identifier, b.Bucket, gf.UUID)
	client := r.Context.S3Clients[b.Provider]
	obj, err := client.GetObject(ctx.Background(), b.Bucket, gf.UUID, minio.GetObjectOptions{})
	if err != nil {
		return digests, err
	}
	defer obj.Close()

	// Add header and file data to tarPipeWriter
	tarHeader := r.GetTarHeader(gf)
	return r.tarPipeWriter.AddFile(tarHeader, obj, r.RestorationObject.ManifestAlgorithms())
}
