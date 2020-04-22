package service

import (
	"sync"
	"time"
)

type OperationResult struct {
	// This is set to true when the process that produces
	// this result starts.
	Attempted bool

	// AttemptNumber is the number of times a worker has attempted
	// this operation.
	AttemptNumber int

	// Errors is a list of ProcessingError objects describing things
	// that went wrong during an operation. Don't write to this. It's
	// public so we can serialize it to/from JSON, but access is locked
	// internally with a mutex.
	Errors []*ProcessingError

	// StartedAt describes when the attempt to read the bag started.
	// If StartedAt.IsZero(), we have not yet attempted to read the
	// bag.
	StartedAt time.Time

	// FinishedAt describes when the attempt to read the bag completed.
	// If FinishedAt.IsZero(), we have not yet attempted to read the
	// bag. Note that the attempt may have completed without succeeding.
	// Check the Succeeded() method to see if the process actually
	// completed successfully.
	FinishedAt time.Time

	// HasFatalError describes whether a worker encountered some
	// fatal error while trying to process an operation. Operations
	// with fatal errors should not be retried by subsequent workers.
	// The most common fatal error is an invalid bag. Other fatal
	// errors, such as Pharos rejecting data about the bag, require
	// admin investigation. Most errors are network related and are
	// non-fatal. They simply require retries.
	HasFatalError bool

	mutex *sync.RWMutex
}

func NewOperationResult() *OperationResult {
	return &OperationResult{
		Attempted:     false,
		AttemptNumber: 0,
		Errors:        make([]*ProcessingError, 0),
		StartedAt:     time.Time{},
		FinishedAt:    time.Time{},
		HasFatalError: false,
		mutex:         &sync.RWMutex{},
	}
}

func (result *OperationResult) Start() {
	result.StartedAt = time.Now().UTC()
}

func (result *OperationResult) Started() bool {
	return !result.StartedAt.IsZero()
}

func (result *OperationResult) Finish() {
	result.FinishedAt = time.Now().UTC()
}

func (result *OperationResult) Finished() bool {
	return !result.FinishedAt.IsZero()
}

func (result *OperationResult) RunTime() time.Duration {
	startTime := result.StartedAt
	if startTime.IsZero() {
		return time.Duration(0)
	}
	endTime := result.FinishedAt
	if endTime.IsZero() {
		endTime = time.Now()
	}
	return endTime.Sub(startTime)
}

func (result *OperationResult) Succeeded() bool {
	result.mutex.RLock()
	succeeded := result.Finished() && len(result.Errors) == 0
	result.mutex.RUnlock()
	return succeeded
}

// A.D. 2019-09-16: Cap total errors at 30.
// In rare cases, ingest server can encounter thousands of read
// errors. If OperationResult captures them all, the data becomes
// too large to post to Pharos.
func (result *OperationResult) AddError(err *ProcessingError) {
	if len(result.Errors) > 29 {
		return
	}
	if err.IsFatal {
		result.HasFatalError = true
	}
	result.mutex.Lock()
	result.Errors = append(result.Errors, err)
	result.mutex.Unlock()
}

func (result *OperationResult) ClearErrors() {
	result.mutex.Lock()
	result.Errors = nil
	result.HasFatalError = false
	result.Errors = make([]*ProcessingError, 0)
	result.mutex.Unlock()
}

func (result *OperationResult) HasErrors() bool {
	result.mutex.RLock()
	hasErrors := len(result.Errors) > 0
	result.mutex.RUnlock()
	return hasErrors
}

func (result *OperationResult) FatalErrors() (errors []*ProcessingError) {
	result.mutex.RLock()
	for _, err := range result.Errors {
		if err.IsFatal {
			errors = append(errors, err)
		}
	}
	result.mutex.RUnlock()
	return errors
}
