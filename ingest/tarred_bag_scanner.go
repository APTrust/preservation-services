package ingest

import (
	"archive/tar"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"github.com/satori/go.uuid"
	"hash"
	"io"
	"os"
	"path"
	"strings"
	"time"
)

// TarredBagScanner reads a tarred BagIt file to collect metadata for
// validation and ingest processing. See ProcessNextEntry() below.
type TarredBagScanner struct {
	IngestObject *service.IngestObject
	reader       io.ReadCloser
	TarReader    *tar.Reader
	TempDir      string
	TempFiles    []string
}

// NewTarredBagScanner creates a new TarredBagScanner.
//
// Param reader is an io.ReadCloser from which to read the tarred
// BagIt file.
//
// Param ingestObject contains info about the bag in the tarred BagIt
// file.
//
// Param tempDir should be the path to a directory in which the scanner
// can temporarily store files it extracts from the tarred bag. These
// files include manifests, tag manifests, and 2-3 tag files. All of the
// extracted files are text files, and they are typically small.
//
// Note that the TarredBagScanner does NOT delete temp files when it's
// done. It stores the paths to the temp files in the TempFiles attribute
// (a string slice). The caller should process the temp files as it pleases,
// and then delete them using this object's DeleteTempFiles method.
//
// For an example of how to use this object, see the ScanBag method in
// https://github.com/APTrust/preservation-services/blob/master/ingest/metadata_gatherer.go
func NewTarredBagScanner(reader io.ReadCloser, ingestObject *service.IngestObject, tempDir string) *TarredBagScanner {
	return &TarredBagScanner{
		IngestObject: ingestObject,
		reader:       reader,
		TarReader:    tar.NewReader(reader),
		TempDir:      tempDir,
		TempFiles:    make([]string, 0),
	}
}

// ProcessNextEntry processes the next file in the tarred bag, returning an
// IngestFile object with metadata about the file. This method returns
// io.EOF after it reads the last file in the tarball. Any error other than
// io.EOF means something went wrong.
//
// This method returns nil, nil for non-file entries such as directories or
// symlinks, neither of which can be usefully archived in S3.
func (scanner *TarredBagScanner) ProcessNextEntry() (ingestFile *service.IngestFile, err error) {
	header, err := scanner.TarReader.Next()
	if err != nil {
		return nil, err
	}
	if header.Typeflag == tar.TypeReg || header.Typeflag == tar.TypeRegA {
		return scanner.processFileEntry(header)
	} else {
		return nil, nil
	}
}

// Process a single file in the tarball.
func (scanner *TarredBagScanner) processFileEntry(header *tar.Header) (*service.IngestFile, error) {
	ingestFile, err := scanner.initIngestFile(header)
	if err != nil {
		return nil, err
	}
	scanner.processFile(ingestFile)
	return ingestFile, nil
}

// Creates an IngestFile object to describe a file in a tarball.
func (scanner *TarredBagScanner) initIngestFile(header *tar.Header) (*service.IngestFile, error) {
	prefix := scanner.IngestObject.BagName() + "/"
	pathInBag := strings.Replace(header.Name, prefix, "", 1)
	if pathInBag == header.Name {
		return nil, fmt.Errorf("Illegal path, '%s'. Should start with '%s'.", header.Name, prefix)
	}
	ingestFile := service.NewIngestFile(scanner.IngestObject.Identifier(), pathInBag)
	ingestFile.Size = header.Size
	ingestFile.UUID = uuid.NewV4().String()

	return ingestFile, nil
}

// Calculates the file's checksums, and saves it to a temp file
// if the file is a manifest, tag manifest, or parsable tag file.
func (scanner *TarredBagScanner) processFile(ingestFile *service.IngestFile) error {
	md5Hash := md5.New()
	sha256Hash := sha256.New()
	writers := []io.Writer{
		md5Hash,
		sha256Hash,
	}
	tempFilePath := scanner.getTempFilePath(ingestFile)
	if tempFilePath != "" {
		tempFile, err := os.Create(tempFilePath)
		if err != nil {
			return fmt.Errorf(
				"Cannot write temp file for ingestFile.Identifier: %s",
				err.Error())
		}
		defer tempFile.Close()
		writers = append(writers, tempFile)
		scanner.TempFiles = append(scanner.TempFiles, tempFilePath)
	}
	multiWriter := io.MultiWriter(writers...)
	_, err := io.Copy(multiWriter, scanner.TarReader)
	if err != nil {
		return err
	}
	scanner.addChecksums(ingestFile, md5Hash, sha256Hash)
	return nil
}

// Adds the checksums to the IngestFile object.
func (scanner *TarredBagScanner) addChecksums(ingestFile *service.IngestFile, md5Hash, sha256Hash hash.Hash) {
	now := time.Now()
	md5Checksum := &service.IngestChecksum{
		Algorithm: constants.AlgMd5,
		DateTime:  now,
		Digest:    fmt.Sprintf("%x", md5Hash.Sum(nil)),
		Source:    constants.SourceIngest,
	}
	sha256Checksum := &service.IngestChecksum{
		Algorithm: constants.AlgSha256,
		DateTime:  now,
		Digest:    fmt.Sprintf("%x", sha256Hash.Sum(nil)),
		Source:    constants.SourceIngest,
	}
	ingestFile.SetChecksum(md5Checksum)
	ingestFile.SetChecksum(sha256Checksum)
}

// Returns a tempfile path for a manifest, tagmanifest, or parsable
// tag file that we want to write to disk for further processing.
// Returns an empty string if we don't need to write this file to
// a tempfile.
func (scanner *TarredBagScanner) getTempFilePath(ingestFile *service.IngestFile) string {
	tempFilePath := ""
	fileType := ingestFile.FileType()
	if fileType == constants.FileTypeManifest ||
		fileType == constants.FileTypeTagManifest ||
		ingestFile.IsParsableTagFile() {
		tempFilePath = path.Join(scanner.TempDir, ingestFile.PathInBag)
	}
	return tempFilePath
}

// CloseReader closes the io.ReadCloser() that was passed into
// NewTarredBagScanner. If you neglect this call in a long-running
// worker process, you'll run the system out of filehandles.
// See also Finish().
func (scanner *TarredBagScanner) CloseReader() {
	if scanner.reader != nil {
		scanner.reader.Close()
	}
}

// DeleteTempFiles deletes all of the temp files that this scanner created.
// See also Finish().
func (scanner *TarredBagScanner) DeleteTempFiles() {
	for _, filepath := range scanner.TempFiles {
		// TODO: what to do on err here?
		if util.LooksSafeToDelete(filepath, 12, 3) {
			_ = os.Remove(filepath)
		}
	}
}

// Finish closes the io.ReadCloser from which the tarred bag was read,
// and it deletes the manifests and tag files that the scanner wrote into
// a temporary directory. Be sure to call this after all calls to
// ProcessNextEntry are complete.
func (scanner *TarredBagScanner) Finish() {
	scanner.CloseReader()
	scanner.DeleteTempFiles()
}
