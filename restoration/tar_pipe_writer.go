package restoration

import (
	"archive/tar"
	"fmt"
	"io"
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
func (w *TarPipeWriter) AddFile(header *tar.Header, r io.Reader) error {

	if err := w.ValidateHeader(header); err != nil {
		return err
	}

	// Write the tar header
	if err := w.tarWriter.WriteHeader(header); err != nil {
		return err
	}

	// Write the file contents
	bytesWritten, err := io.Copy(w.tarWriter, r)
	if bytesWritten != header.Size {
		return fmt.Errorf("AddFile copied only %d of %d bytes for file %s",
			bytesWritten, header.Size, header.Name)
	}
	if err != nil {
		return fmt.Errorf("Error copying %s into tar archive: %v",
			header.Name, err)
	}

	return nil
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

func (w *TarPipeWriter) Finish() {
	w.pipeReader.Close()
	w.pipeWriter.Close()
	w.tarWriter.Close()
}
