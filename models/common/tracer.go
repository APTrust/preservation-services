package common

import (
	"github.com/op/go-logging"
)

var tracer *Tracer

// Tracer lets us write Minio trace output to our logs.
type Tracer struct {
	logger *logging.Logger
}

func GetTracer(logger *logging.Logger) *Tracer {
	if tracer == nil {
		tracer = &Tracer{
			logger: logger,
		}
	}
	return tracer
}

func (t *Tracer) Write(p []byte) (n int, err error) {
	t.logger.Debug(string(p))
	return len(p), nil
}
