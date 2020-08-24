package restoration

import (
	"archive/tar"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"io"

	"github.com/APTrust/preservation-services/constants"
)

// TarPipeWriter writes a tar file through a pipe to any destination
// that accepts an io.Reader. This allows us to write a tar file
// directly to S3.
type TarPipeWriter struct {
	pipeReader *io.PipeReader
	pipeWriter *io.PipeWriter
	tarWriter  *tar.Writer
}

// NewTarPipeWriter creates a new TarPipeWriter.
func NewTarPipeWriter() *TarPipeWriter {
	pipeReader, pipeWriter := io.Pipe()
	return &TarPipeWriter{
		pipeReader: pipeReader,
		pipeWriter: pipeWriter,
		tarWriter:  tar.NewWriter(pipeWriter),
	}
}

// AddFile writes the specified tar header and file data (from reader r)
// into the pipeline.
func (w *TarPipeWriter) AddFile(header *tar.Header, r io.Reader) (digests map[string]string, err error) {

	digests = make(map[string]string, 4)
	if err = w.ValidateHeader(header); err != nil {
		return digests, err
	}

	// Write the tar header
	if err = w.tarWriter.WriteHeader(header); err != nil {
		return digests, err
	}

	md5Hash := md5.New()
	sha1Hash := sha1.New()
	sha256Hash := sha256.New()
	sha512Hash := sha512.New()
	writers := []io.Writer{
		md5Hash,
		sha1Hash,
		sha256Hash,
		sha512Hash,
		w.tarWriter,
	}
	multiWriter := io.MultiWriter(writers...)

	// Write the file contents
	bytesWritten, err := io.Copy(multiWriter, r)
	fmt.Printf("Tar writer wrote %d bytes\n", bytesWritten)
	if bytesWritten != header.Size {
		return digests, fmt.Errorf("AddFile copied only %d of %d bytes for file %s",
			bytesWritten, header.Size, header.Name)
	}
	if err != nil {
		return digests, fmt.Errorf("Error copying %s into tar archive: %v",
			header.Name, err)
	}

	digests[constants.AlgMd5] = fmt.Sprintf("%x", md5Hash.Sum(nil))
	digests[constants.AlgSha1] = fmt.Sprintf("%x", sha1Hash.Sum(nil))
	digests[constants.AlgSha256] = fmt.Sprintf("%x", sha256Hash.Sum(nil))
	digests[constants.AlgSha512] = fmt.Sprintf("%x", sha512Hash.Sum(nil))

	return digests, nil
}

// ValidateHeader returns an error if the tar header is missing a name
// or if its size is less than zero.
func (w *TarPipeWriter) ValidateHeader(header *tar.Header) error {
	if header.Name == "" {
		return fmt.Errorf("Tar header name is missing.")
	}
	// We do have some zero-length files in preservation.
	// Mostly .keep files from Python, PHP and Rails projects.
	if header.Size < 0 {
		return fmt.Errorf("Tar header size cannot be negative for %s.", header.Name)
	}
	return nil
}

// GetReader returns the io.PipeReader, which can be passed to any
// function expecting a reader. Whatever is written into the tar archive
// by AddFile comes out through this reader. You can pass this reader
// to the Minio client's PutObject method, and the write will go straight
// to S3.
func (w *TarPipeWriter) GetReader() *io.PipeReader {
	return w.pipeReader
}

// Finish closes the TarWriter, flushing remaining data. It also closes
// the PipeWriter, which sends an EOF to the PipeReader. Without this,
// the process at the reading end will hang forever, waiting for EOF.
func (w *TarPipeWriter) Finish() {
	w.tarWriter.Close()
	w.pipeWriter.Close()
}
