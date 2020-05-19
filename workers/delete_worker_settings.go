package workers

import (
	"encoding/json"
	"time"
)

// *********************************************************************
// TODO: See if we can merge this with IngestSettings
// *********************************************************************

// DeleteWorkerSettings contains settings for a file/object delete worker.
type DeleteWorkerSettings struct {
	// ChannelBufferSize is the size of the buffer for the
	// ProcessChannel, SuccessChannel, ErrorChannel,
	// and FatalErrorChannel.
	ChannelBufferSize int

	// MaxAttempts is the maximum number of times the worker should
	// attempt its work before giving up. Note that this applies
	// only to attempts that fail from non-fatal (transient) errors.
	// Workers automatically stop trying after fatal errors.
	MaxAttempts int

	// NSQChannel is the NSQ channel the worker should subscribe
	// to to receive messages.
	NSQChannel string

	// NSQTopic is the NSQ topic the worker should subscribe
	// to to receive messages.
	NSQTopic string

	// NumberOfWorkers is the number of go routines to spin up
	// to process deletions.
	NumberOfWorkers int

	// RequeueTimeout describes how long of a timeout to set
	// on the NSQ requeue after an item fails with non-fatal
	// errors.
	RequeueTimeout time.Duration

	// WorkItemSuccessNote is the text to set on the WorkItem.Note
	// after an item has been successfully processed.
	WorkItemSuccessNote string
}

func (settings *DeleteWorkerSettings) ToJSON() string {
	data, _ := json.Marshal(settings)
	return string(data)
}
