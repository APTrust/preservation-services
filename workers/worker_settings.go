package workers

import (
	"encoding/json"
	"time"
)

// Settings contains settings for an ingest worker.
type Settings struct {
	// ChannelBufferSize is the size of the buffer for the
	// ProcessChannel, SuccessChannel, ErrorChannel,
	// and FatalErrorChannel.
	ChannelBufferSize int

	// DeleteFromReceivingAfterFatalError -
	// Set this to true if we should delete the original bag
	// from the depositor's receiving bucket after a fatal error.
	// We do want to do this if the bag is invalid. In most other
	// cases, we want to avoid this.
	DeleteFromReceivingAfterFatalError bool

	// DeleteFromReceivingAfterMaxFatalAttempts -
	// Set this to true if we should delete the original bag
	// from the depositor's receiving bucket after the
	// processor exceeds the max number of fail attempts.
	// Note that these are attempts that failed due to
	// non-fatal errors. This should almost always be false.
	DeleteFromReceivingAfterMaxFailedAttempts bool

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

	// NextQueueTopic is the name of the NSQ topic to which a
	// WorkItem should be pushed after a worker successfully
	// processes it. You can set to to an empty string if the
	// item should not be pushed to any topic upon completion.
	// (This is the case with the Cleanup worker, after which
	// there is no further work to be done.)
	NextQueueTopic string

	// NextWorkItemStage is the name of the stage to set on the
	// WorkItem.Stage property when this worker successfully
	// completes.
	NextWorkItemStage string

	// NumberOfWorkers is the number of go routines to spin up
	// to handle the main task of the worker. That task is the
	// Run() method of an ingest.Base object. Depending on the
	// nature of the work, the Run() method may be CPU-, memory-,
	// or network-intensive. Setting NumberOfWorkers too high
	// will overtax one or two of those resources. Setting it
	// too low will not make efficient use of resources. The
	// resource most likely to max out will be network bandwidth
	// in the workers that have a lot of S3 interaction.
	NumberOfWorkers int

	// PushToCleanupAfterMaxFailedAttempts describes whether
	// we should push the item into the NSQ cleanup topic
	// after MaxAttempts have failed due to non-fatal errors.
	// This should almost always be false.
	PushToCleanupAfterMaxFailedAttempts bool

	// PushToCleanupOnFatalError describes whether we should
	// push an item in the NSQ cleanup topic after a fatal
	// processing error. In most cases, this should be false.
	// If a bag is invalid, it should be true, because we cannot
	// ingest an invalid bag.
	PushToCleanupOnFatalError bool

	// RequeueTimeout describes how long of a timeout to set
	// on the NSQ requeue after an item fails with non-fatal
	// errors.
	RequeueTimeout time.Duration

	// WorkItemSuccessNote is the text to set on the WorkItem.Note
	// after an item has been successfully processed.
	WorkItemSuccessNote string
}

func (settings *Settings) ToJSON() string {
	data, _ := json.Marshal(settings)
	return string(data)
}
