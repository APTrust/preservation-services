package logger

import (
	"fmt"
	"io/ioutil"
	stdlog "log"
	"os"
	"path"
	"path/filepath"

	"github.com/op/go-logging"
)

var log *logging.Logger
var filename string

// InitLogger creates and returns a logger suitable for logging
// human-readable messages. Also returns the path to the log file.
func InitLogger(logDir string, logLevel logging.Level) (*logging.Logger, string) {
	if log != nil {
		return log, filename
	}
	processName := path.Base(os.Args[0])
	filename = fmt.Sprintf("%s.log", processName)
	filename = filepath.Join(logDir, filename)
	writer, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot open log file '%s': %v\n", filename, err)
		os.Exit(1)
	}
	log = logging.MustGetLogger(processName)
	format := logging.MustStringFormatter("[%{level}] %{message}")
	logging.SetFormatter(format)
	logging.SetLevel(logLevel, processName)
	logBackend := logging.NewLogBackend(writer, "", stdlog.LstdFlags|stdlog.LUTC)
	logging.SetBackend(logBackend)
	return log, filename
}

// DiscardLogger returns a logger that writes to dev/null.
func DiscardLogger(module string) *logging.Logger {
	log := logging.MustGetLogger(module)
	devnull := logging.NewLogBackend(ioutil.Discard, "", 0)
	logging.SetBackend(devnull)
	logging.SetLevel(logging.INFO, "volume_test")
	return log
}
