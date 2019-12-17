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
	TarReader    *tar.Reader
	IngestObject *ingest.IngestObject
}

func NewPreProcessor(reader *io.Reader, ingestObject *ingest.IngestObject) *PreProcessor {
	return &PreProcessor{
		TarReader:    tar.NewReader(reader),
		IngestObject: ingestObject,
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
		p.addChecksums(ingestFile)
	}
	return ingestFile, nil
}

func (p *PreProcessor) initIngestFile(header *tar.Header) (ingestFile *ingest.IngestFile, err error) {
	fileIdentifier := fmt.Sprintf(p.IngestObject.Identifier, header.Name)
	ingestFile = ingest.NewIngestFile(fileIdentifier)
	ingestFile.Size = header.Size
	ingestFile.Mode = header.Mode
	ingestFile.Uid = header.Uid
	ingestFile.Gid = header.Gid
	ingestFile.Uname = header.Uname
	ingestFile.Gname = header.Gname
	return ingestFile
}

func (p *PreProcessor) addChecksums(header *tar.Header) error {
	md5Hash := md5.New()
	sha256Hash := sha256.New()
	multiWriter := io.MultiWriter(md5Hash, sha256Hash)
	_, err := io.Copy(multiWriter, tarReader)
	if err != nil {
		return err
	}
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
}

// Need to read manifests, tag manifests, and tag files.
