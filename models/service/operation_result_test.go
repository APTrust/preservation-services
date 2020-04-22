package service_test

import (
	"github.com/APTrust/preservation-services/models/service"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var nonFatalErr = service.NewProcessingError(999, "test.edu/obj", "Non-fatal error", false)
var fatalErr = service.NewProcessingError(333, "test.edu/obj", "Fatal error", true)

func TestNewResult(t *testing.T) {
	result := service.NewOperationResult()
	assert.False(t, result.Attempted)
	assert.EqualValues(t, 0, result.AttemptNumber)
	assert.NotNil(t, result.Errors)
	assert.Equal(t, 0, len(result.Errors))
	assert.True(t, result.StartedAt.IsZero())
	assert.True(t, result.FinishedAt.IsZero())
	assert.False(t, result.HasFatalError)
}

func TestResultStart(t *testing.T) {
	result := service.NewOperationResult()
	assert.True(t, result.StartedAt.IsZero())
	result.Start()
	assert.False(t, result.StartedAt.IsZero())
}

func TestResultStarted(t *testing.T) {
	result := service.NewOperationResult()
	assert.False(t, result.Started())
	result.Start()
	assert.True(t, result.Started())
}

func TestResultFinish(t *testing.T) {
	result := service.NewOperationResult()
	assert.True(t, result.FinishedAt.IsZero())
	result.Finish()
	assert.False(t, result.FinishedAt.IsZero())
}

func TestResultFinished(t *testing.T) {
	result := service.NewOperationResult()
	result.Finish()
	assert.True(t, result.Finished())
}

func TestResultRuntime(t *testing.T) {
	result := service.NewOperationResult()
	now := time.Now()
	fiveMinutesAgo := now.Add(-5 * time.Minute)
	result.StartedAt = fiveMinutesAgo
	result.FinishedAt = now
	assert.EqualValues(t, 5*time.Minute, result.RunTime())
}

func TestResultSucceeded(t *testing.T) {
	result := service.NewOperationResult()

	// Not finished.
	assert.False(t, result.Succeeded())

	// Finished with no errors
	result.Finish()
	assert.True(t, result.Succeeded())

	// Finished with errors
	result.AddError(nonFatalErr)
	assert.False(t, result.Succeeded())
}

func TestAddError(t *testing.T) {
	result := service.NewOperationResult()
	result.AddError(nonFatalErr)
	assert.Equal(t, 1, len(result.Errors))
	result.AddError(nonFatalErr)
	assert.Equal(t, 2, len(result.Errors))
}

func TestAddError_Limit(t *testing.T) {
	result := service.NewOperationResult()
	for i := 0; i < 40; i++ {
		result.AddError(nonFatalErr)
	}
	assert.Equal(t, len(result.Errors), 30)
}

func TestHasErrors(t *testing.T) {
	result := service.NewOperationResult()
	assert.False(t, result.HasErrors())
	result.AddError(nonFatalErr)
	assert.True(t, result.HasErrors())
}

func TestClearErrors(t *testing.T) {
	result := service.NewOperationResult()
	result.AddError(fatalErr)
	assert.True(t, result.HasFatalError)
	assert.NotEmpty(t, result.Errors)
	result.ClearErrors()
	assert.Empty(t, result.Errors)
	assert.False(t, result.HasFatalError)
}

func TestFatalErrors(t *testing.T) {
	result := service.NewOperationResult()

	result.AddError(nonFatalErr)
	assert.Equal(t, 1, len(result.Errors))
	assert.False(t, result.HasFatalError)

	result.AddError(fatalErr)
	assert.True(t, result.HasFatalError)

	result.AddError(nonFatalErr)
	result.AddError(fatalErr)

	fatalErrors := result.FatalErrors()
	assert.Equal(t, 2, len(fatalErrors))
	for _, err := range fatalErrors {
		assert.True(t, err.IsFatal)
	}
}
