package common

import (
	"fmt"
	"runtime"
)

type DetailedError interface {
	Detail() string
}

// Error is a custom error type that includes some additional fields
// to help us debug. See the Detail method.
type Error struct {
	Err     error
	File    string
	IsFatal bool
	Line    int
	Message string
}

func NewError(message string, err error, isFatal bool) *Error {
	_, file, line, _ := runtime.Caller(0)
	return &Error{
		Err:     err,
		File:    file,
		IsFatal: isFatal,
		Line:    line,
		Message: message,
	}
}

func (e *Error) Unwrap() error {
	return e.Err
}

func (e *Error) Error() string {
	return e.Message
}

// This returns a detailed error message.
func (e *Error) Detail() string {
	prefix := ""
	if e.IsFatal {
		prefix = "FATAL: "
	}
	underlyingError := ""
	if e.Err != nil {
		underlyingError = fmt.Sprintf("(Underlying error: %s)", e.Err.Error())
	}
	return fmt.Sprintf("%s%s [%s:%d] %s",
		prefix, e.Message, e.File, e.Line, underlyingError)
}

// HTTPError is a custom error struct that captures details of errors
// coming from Pharos, Redis, and S3.
type HTTPError struct {
	Err        error
	Message    string
	Method     string
	StatusCode int
	URL        string
}

func NewHTTPError(message string, err error, method, url string, statusCode int) *HTTPError {
	return &HTTPError{
		Err:        err,
		Message:    message,
		Method:     method,
		URL:        url,
		StatusCode: statusCode,
	}
}

func (e *HTTPError) Unwrap() error {
	return e.Err
}

func (e *HTTPError) Error() string {
	return e.Message
}

func (e *HTTPError) Detail() string {
	underlyingError := ""
	if e.Err != nil {
		underlyingError = fmt.Sprintf("(Underlying error: %s)", e.Err.Error())
	}
	return fmt.Sprintf(
		"%s: %s returned status %d. Message: %s %s",
		e.Method, e.URL, e.StatusCode, e.Message, underlyingError)
}
