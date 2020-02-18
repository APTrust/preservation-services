package common

import (
	"fmt"
	"runtime"
)

type FatalError struct {
	Err error
}

func newFatalError(message string, err error) *FatalError {
	return &FatalError{
		Err: fmt.Errorf("%s: %w", message, err),
	}
}

func (e *FatalError) Error() string {
	return "Fatal: " + e.Err.Error()
}

func (e *FatalError) Unwrap() error {
	return e.Err
}

type CustomError struct {
	File    string
	Line    int
	Err     error
	Message string
}

func NewError(message string, err error, isFatal bool) *CustomError {
	_, file, line, _ := runtime.Caller(0)
	if isFatal {
		err = newFatalError(message, err)
	}
	return &CustomError{
		File:    file,
		Line:    line,
		Err:     err,
		Message: message,
	}
}

func (e *CustomError) Unwrap() error {
	return e.Err
}

func (e *CustomError) Error() string {
	return e.Message
}
