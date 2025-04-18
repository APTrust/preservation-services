package restoration

import (
	"archive/tar"
	"bytes"
	ctx "context"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/APTrust/preservation-services/bagit"
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

	// Add one for bagit.txt, which was not in preservation storage,
	// but added above in the call to r.AddBagItFile().
	fileCount++

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

		// Note: Minio docs discourage passing object size of -1,
		// as we do in the call to PutObject below, but it's
		// impossible for us to predict the exact size of the
		// restored bag because sizes of tag files and manifests
		// vary.
		var uploadInfo minio.UploadInfo
		uploadInfo, r.uploadError = s3Client.PutObject(
			ctx.Background(),
			r.RestorationObject.RestorationTarget,
			r.RestorationObject.Identifier+".tar",
			r.tarPipeWriter.GetReader(),
			-1,
			r.Context.Config.MinioDefaultPutOptions,
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
	isObjectRestoration := r.RestorationObject.RestorationType == constants.RestorationTypeObject
	fileCount = 0
	pageNumber := 1
	for {
		files, err := GetBatchOfFiles(r.Context, r.RestorationObject.ItemID, pageNumber)
		if err != nil {
			errors = append(errors, r.Error(r.RestorationObject.Identifier, err, false))
			return fileCount, errors
		}
		for _, gf := range files {
			var digests map[string]string
			filename, _ := gf.PathInBag()
			if isObjectRestoration && filename == "bag-info.txt" {
				digests, err = r.RewriteBagInfo(gf)
				//
				// Add bag-info.txt to tag manifests without comparing its checksums
				// to Registry checksums. They won't match, because we just rewrote bag-info.txt.
				// https://github.com/APTrust/preservation-services/issues/134
				//
				for _, alg := range constants.SupportedManifestAlgorithms {
					digest := digests[alg]
					// If bagger didn't calculate this alg, it wasn't part of the BagIt profile.
					if digest == "" {
						continue
					}
					err = r.AppendDigestToManifest(gf, digest, alg)
				}
			} else {
				digests, err = r.AddToTarFile(gf)
			}
			if err != nil {
				r.Context.Logger.Errorf("Error adding %s: %v", gf.Identifier, err)
				errors = append(errors, r.Error(gf.Identifier, err, true))
				return fileCount, errors
			} else {
				r.Context.Logger.Infof("Added %s", gf.Identifier)
			}
			// Verify checksums on all files, except bag-info.txt. That one will not
			// match what's in the registry, because we've rewritten it.
			if !isObjectRestoration || (isObjectRestoration && filename != "bag-info.txt") {
				err = r.RecordDigests(gf, digests)
				if err != nil {
					r.Context.Logger.Errorf("Error recording digests for %s: %v", gf.Identifier, err)
					errors = append(errors, r.Error(gf.Identifier, err, true))
					return fileCount, errors
				}
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
	atLeastOneChecksumVerified := false
	for _, alg := range constants.SupportedManifestAlgorithms {
		digest := digests[alg]
		// The checksums in the digests map include only those algorithms required
		// by the BagIt profile. This list may not include all of our supported
		// algorithms. If the bagger didn't calculate this particular algorithm, it's
		// because it didn't have to. Move on.
		if digest == "" {
			continue
		}
		registryChecksum := gf.GetLatestChecksum(alg)
		if registryChecksum != nil && digest != registryChecksum.Digest {
			return fmt.Errorf("%s digest mismatch for %s. Registry says %s, S3 file has %s", alg, gf.Identifier, registryChecksum.Digest, digest)
		}
		atLeastOneChecksumVerified = true
		err := r.AppendDigestToManifest(gf, digest, alg)
		if err != nil {
			return err
		}
	}
	if !atLeastOneChecksumVerified {
		return fmt.Errorf("BagRestorer.RecordDigests was not able to verify any checksums for %s", gf.Identifier)
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
	obj, digests, err := r.getS3Object(gf)
	if err != nil {
		return digests, err
	}
	defer obj.Close()
	// Add header and file data to tarPipeWriter
	tarHeader := r.GetTarHeader(gf)
	return r.tarPipeWriter.AddFile(tarHeader, obj, r.RestorationObject.ManifestAlgorithms())
}

// RewriteBagInfo updates this bag's bag-info.txt file. Depositors want
// us to preserve this file because it often contains meaningful information
// that helps with local restoration. However, we need to rewrite outdated
// tag values when we restore. Specifically:
//
// 1. Payload-Oxum. If the depositor added or deleted files after the initial
// ingest of this bag, the file count and byte count of the Payload-Oxum will
// not match what's in the restored bag, and validators will reject the bag as
// invalid. Payload-Oxum must match what we're actually restoring.
//
// 2. Bagging-Software. The original bag was not made with our restoration
// bagger. The restored bag is.
//
// 3. Bagging-Date. This should be set to today, not to the original bag date.
//
// 4. Bag-Size. This may have changed if files were added or deleted after
// initial ingest.
//
// We can preserve the original tags by prepending "Original-" to their names.
func (r *BagRestorer) RewriteBagInfo(gf *registry.GenericFile) (digests map[string]string, err error) {
	obj, digests, err := r.getS3Object(gf)
	if err != nil {
		return digests, err
	}
	defer obj.Close()

	// Normally, it would be dangerous to read an S3 object into a buffer
	// because it could run us out of memory. However, bag-info.txt files
	// tend to be < 1kb in size, maxing out at around 20kb.
	buf := make([]byte, gf.Size)
	n, err := obj.Read(buf)
	if n != int(gf.Size) {
		return digests, fmt.Errorf("during restoration, error reading bag-info.txt from preservation bucket: read only %d of %d bytes", n, gf.Size)
	}
	if err != nil && err != io.EOF {
		return digests, fmt.Errorf("during restoration, error reading bag-info.txt from preservation bucket: %v", err)
	}
	reader := bytes.NewReader(buf)
	tags, err := bagit.ParseTagFile(reader, "bag-info.txt")

	resp := r.Context.RegistryClient.IntellectualObjectByID(r.RestorationObject.ItemID)
	if resp.Error != nil {
		return digests, fmt.Errorf("error during bag-info.txt rewrite: %v", resp.Error)
	}
	intelObj := resp.IntellectualObject()
	newTags := RewriteTags(tags, intelObj.PayloadSize, intelObj.PayloadFileCount)

	// Write tags out to string or buffer that supports Read() and Seek()
	readSeeker, byteCount := TagsToReadSeeker(newTags)

	// Now add it and the header to tarPipeWriter. We have to alter
	// gf.Size before we get the tar header, because our rewritten tag
	// file is longer than the original. And be sure to reset gf.Size
	// afterward, so we don't accidentally persist incorrect data to
	// Redis or the Registry DB.
	oldSize := gf.Size
	gf.Size = byteCount
	defer func() { gf.Size = oldSize }()
	tarHeader := r.GetTarHeader(gf)
	return r.tarPipeWriter.AddFile(tarHeader, readSeeker, r.RestorationObject.ManifestAlgorithms())
}

func RewriteTags(tags []*bagit.Tag, objSize, fileCount int64) []*bagit.Tag {
	oxum := fmt.Sprintf("%d.%d", objSize, fileCount)
	originalTags := make([]*bagit.Tag, 0)
	for _, tag := range tags {
		if tag.TagName == "Payload-Oxum" {
			originalTags = append(originalTags, bagit.NewTag("bag-info.txt", "Original-Payload-Oxum", tag.Value))
			tag.Value = oxum
		} else if tag.TagName == "Bagging-Date" {
			originalTags = append(originalTags, bagit.NewTag("bag-info.txt", "Original-Bagging-Date", tag.Value))
			tag.Value = time.Now().UTC().Format(time.RFC3339)
		} else if tag.TagName == "Bag-Size" {
			originalTags = append(originalTags, bagit.NewTag("bag-info.txt", "Original-Bag-Size", tag.Value))
			tag.Value = util.ToHumanSize(objSize)
		} else if tag.TagName == "Bagging-Software" {
			originalTags = append(originalTags, bagit.NewTag("bag-info.txt", "Original-Bagging-Software", tag.Value))
			tag.Value = constants.RestorationBaggingSoftware
		}
	}
	return append(tags, originalTags...)
}

// TagsToReadSeeker converts a slice of Tags to a ReadSeeker that we can
// use to copy data into our tar file. This also returns the length of
// the newly serialized tags.
func TagsToReadSeeker(tags []*bagit.Tag) (io.ReadSeeker, int64) {
	lines := make([]string, len(tags))
	for i, tag := range tags {
		lines[i] = fmt.Sprintf("%s: %s", tag.TagName, tag.Value)
	}
	serializedTags := strings.Join(lines, "\n")
	return strings.NewReader(serializedTags), int64(len([]byte(serializedTags)))
}

// getS3Object returns a *minio.Object and digest map for the specified
// GenericFile, so we can stream it to wherever it needs to go.
func (r *BagRestorer) getS3Object(gf *registry.GenericFile) (obj *minio.Object, digests map[string]string, err error) {
	digests = make(map[string]string)
	b, _, err := BestRestorationSource(r.Context, gf)
	if err != nil {
		return nil, digests, err
	}
	r.Context.Logger.Infof("Getting %s from %s with UUID %s", gf.Identifier, b.Bucket, gf.UUID)
	client := r.Context.S3Clients[b.Bucket]
	obj, err = client.GetObject(ctx.Background(), b.Bucket, gf.UUID, minio.GetObjectOptions{})
	return obj, digests, err
}
