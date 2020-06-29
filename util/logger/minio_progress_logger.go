package logger

import (
	"github.com/op/go-logging"
)

// MinioProgressLogger logs the progress of Minio's ComposeObjectWithProgress,
// which performs server-side copies of S3 objects.
type MinioProgressLogger struct {
	logger         *logging.Logger
	chunkNumber    int
	totalBytes     int
	fileSize       int64
	lastPctPrinted float64
	prefix         string
}

const _100MB = int64(104857600)
const _1GB = int64(1073741824)
const _10GB = int64(10737418240)

// NewMinioProgressLogger creates a new MinioProgressLogger.
func NewMinioProgressLogger(logger *logging.Logger, prefix string, fileSize int64) *MinioProgressLogger {
	return &MinioProgressLogger{
		logger:         logger,
		prefix:         prefix,
		chunkNumber:    1,
		totalBytes:     0,
		lastPctPrinted: 0.0,
		fileSize:       fileSize,
	}
}

// Read fulfills the io.Reader interface required by Minio's
// ComposeObjectWithProgress. This reader prints progress updates
// into the worker log, trying not to be too verbose.
func (e *MinioProgressLogger) Read(p []byte) (n int, err error) {
	numBytes := len(p)
	pctComplete := ((float64(numBytes) / float64(e.fileSize)) * 100) + e.lastPctPrinted
	e.totalBytes += numBytes

	if e.shouldPrint(pctComplete) {
		e.logger.Infof("%s : chunk %d, %d of %d bytes, %3.2f%% complete",
			e.prefix, e.chunkNumber, len(p), e.fileSize, pctComplete)
	}

	e.lastPctPrinted = pctComplete
	e.chunkNumber++
	return len(p), nil
}

// shouldPrint returns true if the logger should print a message to the log.
// We generally don't need to log small files, because they upload quickly.
// Very large files may be broken into 10,000 parts. Minio logs the completion
// of each part, but we really don't want 10K log entries for a single upload.
// So we log only meaningful progress.
func (e *MinioProgressLogger) shouldPrint(pctComplete float64) bool {
	diff := pctComplete - e.lastPctPrinted
	if e.fileSize > _10GB {
		return diff >= float64(1.0)
	}
	if e.fileSize > _1GB {
		return diff >= 5.0
	}
	if e.fileSize > _100MB {
		return diff >= 20.0
	}
	return false
}
