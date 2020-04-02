package service

import (
	"fmt"
	"runtime"
)

type ProcessingError struct {
	Identifier string
	IsFatal    bool
	Message    string
	Source     string
	WorkItemID int
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
func NewProcessingError(workItemID int, identifier, message string, isFatal bool) *ProcessingError {
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
		"(identifier: %s) (source: %s)", e.WorkItemID, e.Message,
		severity, e.Identifier, source)
}
