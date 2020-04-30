package service

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"time"
)

type WorkResult struct {
	// Attempt is the number of the attempt to do this work.
	Attempt int `json:"attempt"`

	// Operation is the name of the operation: pre-fetch, validation,
	// storage, etc.
	Operation string `json:"operation"`

	// Host is the name of the network host on which the worker is running.
	Host string `json:"host"`

	// Pid is the pid of the worker doing this work.
	Pid int `json:"pid"`

	// StartedAt describes when the attempt to read the bag started.
	// If StartedAt.IsZero(), we have not yet attempted to read the
	// bag.
	StartedAt time.Time `json:"started_at"`

	// FinishedAt describes when the attempt to read the bag completed.
	// If FinishedAt.IsZero(), we have not yet attempted to read the
	// bag. Note that the attempt may have completed without succeeding.
	// Check the Succeeded() method to see if the process actually
	// completed successfully.
	FinishedAt time.Time `json:"finished_at"`

	// Errors is a list of ProcessingError objects describing things
	// that went wrong during an operation. Don't write to this. It's
	// public so we can serialize it to/from JSON, but access is locked
	// internally with a mutex.
	Errors []*ProcessingError `json:"errors"`

	mutex *sync.RWMutex
}

func NewWorkResult(operation string) *WorkResult {
	hostname, _ := os.Hostname()
	return &WorkResult{
		Operation: operation,
		Host:      hostname,
		Pid:       os.Getpid(),
		Errors:    make([]*ProcessingError, 0),
		mutex:     &sync.RWMutex{},
	}
}

func (result *WorkResult) Start() {
	result.StartedAt = time.Now().UTC()
}

func (result *WorkResult) Started() bool {
	return !result.StartedAt.IsZero()
}

func (result *WorkResult) Finish() {
	result.FinishedAt = time.Now().UTC()
}

func (result *WorkResult) Finished() bool {
	return !result.FinishedAt.IsZero()
}

func (result *WorkResult) RunTime() time.Duration {
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

func (result *WorkResult) Succeeded() bool {
	result.mutex.RLock()
	succeeded := result.Finished() && len(result.Errors) == 0
	result.mutex.RUnlock()
	return succeeded
}

// AddError adds a ProcessingError to the result. The total number of
// errors is capped at 30, unless the error being added is fatal.
// The error cap exists because often a network connection problem will
// result in the same non-fatal error occurring hundred of times. We get
// the point after 2 or 3, and we don't need to serialize 500 errors to
// JSON. We must add fatal errors no matter what. Processing typically
// stops on the first fatal error, so there will rarely be more than one.
// (Bag validation errors are an exception, since a bag may have multiple
// problems and all of them are fatal.)
func (result *WorkResult) AddError(err *ProcessingError) {
	if len(result.Errors) > 29 && !err.IsFatal {
		return
	}
	result.mutex.Lock()
	result.Errors = append(result.Errors, err)
	result.mutex.Unlock()
}

func (result *WorkResult) ClearErrors() {
	result.mutex.Lock()
	result.Errors = nil
	result.Errors = make([]*ProcessingError, 0)
	result.mutex.Unlock()
}

// Reset clears everything but the attempt number and the operation name.
func (result *WorkResult) Reset() {
	result.Host = ""
	result.Pid = 0
	result.StartedAt = time.Time{}
	result.FinishedAt = time.Time{}
	result.ClearErrors()
}

// HasErrors returns true if this result has any errors,
// fatal or not.
func (result *WorkResult) HasErrors() bool {
	result.mutex.RLock()
	hasErrors := len(result.Errors) > 0
	result.mutex.RUnlock()
	return hasErrors
}

// FatalErrors returns a list of all of this result's fatal errors.
func (result *WorkResult) FatalErrors() (errors []*ProcessingError) {
	result.mutex.RLock()
	for _, err := range result.Errors {
		if err.IsFatal {
			errors = append(errors, err)
		}
	}
	result.mutex.RUnlock()
	return errors
}

// HasFatalErrors returns true if this result has any fatal errors.
func (result *WorkResult) HasFatalErrors() bool {
	return len(result.FatalErrors()) > 0
}

// FatalErrorMessage returns all fatal error messages as a single
// pipe-demilimited string.
func (result *WorkResult) FatalErrorMessage() string {
	errors := result.FatalErrors()
	messages := make([]string, len(errors))
	for i, err := range errors {
		messages[i] = err.Message
	}
	return strings.Join(messages[:], " | ")
}

// WorkResultFromJSON converts the JSON representation of a WorkResult
// into a full-fledged object. Note that this involves not only deserializing
// the JSON, but also initializing an internal mutex. If you deserialize
// without this function, you'll eventually run into nil pointer exceptions
// because the mutex won't exist.
func WorkResultFromJSON(jsonData string) (*WorkResult, error) {
	result := &WorkResult{}
	err := json.Unmarshal([]byte(jsonData), result)
	if err != nil {
		return nil, err
	}
	result.mutex = &sync.RWMutex{}
	return result, nil
}

func (result *WorkResult) ToJSON() (string, error) {
	bytes, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
