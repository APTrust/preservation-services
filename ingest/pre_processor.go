package ingest

import (
	"archive/tar"
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/ingest"
	"io"
)

type PreProcessor struct {
	IngestObject *ingest.IngestObject
	TarReader    *tar.Reader
	TempDir      string
	TempFiles    []string
}

func NewPreProcessor(reader *io.Reader, ingestObject *ingest.IngestObject, tempDir string) *PreProcessor {
	return &PreProcessor{
		IngestObject: ingestObject,
		TarReader:    tar.NewReader(reader),
		TempDir:      tempDir,
		TempFiles:    make([]string, 0),
	}
}

func (p *PreProcessor) ProcessNextEntry(tarReader *tar.Reader) (ingestFile *ingest.IngestFile, err error) {
	header, err := p.TarReader.Next()
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if header.Typeflag == tar.TypeReg || header.Typeflag == tar.TypeRegA {
		ingestFile = p.initIngestFile(header)
		p.processFile(ingestFile)
	}
	return ingestFile, nil
}

func (p *PreProcessor) initIngestFile(header *tar.Header) (ingestFile *ingest.IngestFile, err error) {
	fileIdentifier := fmt.Sprintf(p.IngestObject.Identifier, header.Name)
	ingestFile = ingest.NewIngestFile(fileIdentifier)
	ingestFile.Size = header.Size
	return ingestFile
}

// Calculates the file's checksums, and saves it to a temp file
// if the file is a manifest, tag manifest, or parsable tag file.
func (p *PreProcessor) processFile(ingestFile *ingest.IngestFile) error {
	md5Hash := md5.New()
	sha256Hash := sha256.New()
	writers := []*io.Writer{
		md5Hash,
		sha256Hash,
	}
	tempFilePath := p.getTempFilePath(ingestFile)
	if tempFilePath != "" {
		tempFile, err := os.Create(tempFilePath)
		if err != nil {
			return fmt.Errorf(
				"Cannot write temp file for ingestFile.Identifier: %s",
				err.Error())
		}
		defer tempFile.Close()
		writers = append(writers, tempFile)
		p.TempFiles = append(p.TempFiles, tempFilePath)
	}
	multiWriter := io.MultiWriter(writers...)
	_, err := io.Copy(multiWriter, tarReader)
	if err != nil {
		return err
	}
	p.addChecksums(ingestFile, md5Hash, sha256Hash)
	return nil
}

// Adds the checksums to the IngestFile object.
func (p *PreProcessor) addChecksums(ingestFile *ingest.IngestFile, md5Hash, sha256Hash hash.Hash) {
	now := time.Now()
	md5Checksum := &IngestChecksum{
		Algorithm: constants.AlgMd5,
		DateTime:  now,
		Digest:    fmt.Sprintf("%x", md5Hash.Sum(nil)),
		Source:    constants.SourceIngest,
	}
	sha256Digest = &IngestChecksum{
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
func (p *PreProcessor) getTempFilePath(ingestFile *ingest.IngestFile) string {
	tempFilePath := ""
	fileType := ingestFile.FileType()
	if fileType == constants.FileTypeManifest ||
		fileType == FileTypeTagManifest ||
		ingestFile.IsParsableTagFile() {
		tempFilePath = path.Join(p.TempDir, ingestFile.PathInBag)
	}
	return tempFilePath
}
