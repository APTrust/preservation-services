package logger

import (
	"fmt"
	"github.com/op/go-logging"
	stdlog "log"
	"os"
	"path"
	"path/filepath"
)

/*
InitLogger creates and returns a logger suitable for logging
human-readable message. Also returns the path to the log file.
*/
func InitLogger(logDir string, logLevel logging.Level) (*logging.Logger, string) {
	processName := path.Base(os.Args[0])
	filename := fmt.Sprintf("%s.log", processName)
	filename = filepath.Join(logDir, filename)
	writer, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot open log file '%s': %v\n", filename, err)
		os.Exit(1)
	}
	log := logging.MustGetLogger(processName)
	format := logging.MustStringFormatter("[%{level}] %{message}")
	logging.SetFormatter(format)
	logging.SetLevel(logLevel, processName)
	logBackend := logging.NewLogBackend(writer, "", stdlog.LstdFlags|stdlog.LUTC)
	logging.SetBackend(logBackend)
	return log, filename
}
