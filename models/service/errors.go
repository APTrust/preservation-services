package service

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// ProcessingError contains detailed information about an error that
// occurred during Ingest, Restoration, Deletion, or Fixity Checking.
type ProcessingError struct {

	// Identifier is a file identifier or object identifier, depending
	// on whether we're processing an object or a file.
	Identifier string

	// IsFatal describes whether or not an error is fatal. Most are
	// transient, such as network errors. A fatal error indicates no
	// further processing should occur.
	IsFatal bool

	// Message is the text of the error.
	Message string

	// Source contains the file name and line number where the error
	// occurred.
	Source string

	// Stack is the stack trace.
	Stack string

	// WorkItemID is the ID of the WorkItem being processed when the
	// error occurree.
	WorkItemID int64
}

// NewProcessingError returns a new ProcessingError. Param workItemID
// is the ID of the WorkItem being processed when the error occurred.
// Param identifier can be an objectidentifier or a file identifier.
// Param message is the error message. Param isFatal describes whether
// the error is fatal. Fatal errors are those
// which will prevent a worker from succeeding when it tries to reprocess
// a WorkItem. Non-fatal errors are transient. For example, an invalid bag
// is a fatal error because it will still be invalid the next time we look
// at it. Network errors are transient and are likely to succeed on future
// tries. We may flag transient errors as fatal after too many retries.
// For example, repeated failed attempts to connect to a network host should
// be flagged as fatal so an admin can look into the issue.
func NewProcessingError(workItemID int64, identifier, message string, isFatal bool) *ProcessingError {
	_, filename, line, ok := runtime.Caller(1)
	source := "unknown:0"
	if ok {
		source = fmt.Sprintf("%s:%d", filename, line)
	}
	return &ProcessingError{
		Identifier: identifier,
		IsFatal:    isFatal,
		Message:    message,
		Source:     source,
		Stack:      string(debug.Stack()),
		WorkItemID: workItemID,
	}
}

func (e *ProcessingError) Error() string {
	severity := "non-fatal"
	if e.IsFatal {
		severity = "fatal"
	}
	source := "unknown:0"
	if e.Source != "" {
		source = e.Source
	}
	return fmt.Sprintf("(workitem %d) (message: %s) (severity: %s) "+
		"(identifier: %s) (source: %s) (stack: %s)", e.WorkItemID, e.Message,
		severity, e.Identifier, source, e.Stack)
}
