package service_test

import (
	"os"
	"testing"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var nonFatalErr = service.NewProcessingError(999, "test.edu/obj", "Non-fatal error", false)
var fatalErr = service.NewProcessingError(333, "test.edu/obj", "Fatal error", true)

func TestNewResult(t *testing.T) {
	result := service.NewWorkResult("op-name")
	assert.EqualValues(t, 0, result.Attempt)
	assert.NotNil(t, result.Errors)
	assert.Equal(t, 0, len(result.Errors))
	assert.True(t, result.StartedAt.IsZero())
	assert.True(t, result.FinishedAt.IsZero())
	assert.False(t, result.HasFatalErrors())
	assert.Equal(t, "op-name", result.Operation)
	assert.Equal(t, os.Getpid(), result.Pid)
	hostname, _ := os.Hostname()
	assert.NotEmpty(t, result.Host)
	assert.Equal(t, hostname, result.Host)
}

func TestResultStart(t *testing.T) {
	result := service.NewWorkResult("op-name")
	assert.True(t, result.StartedAt.IsZero())
	result.Start()
	assert.False(t, result.StartedAt.IsZero())
}

func TestResultStarted(t *testing.T) {
	result := service.NewWorkResult("op-name")
	assert.False(t, result.Started())
	result.Start()
	assert.True(t, result.Started())
}

func TestResultFinish(t *testing.T) {
	result := service.NewWorkResult("op-name")
	assert.True(t, result.FinishedAt.IsZero())
	result.Finish()
	assert.False(t, result.FinishedAt.IsZero())
}

func TestResultFinished(t *testing.T) {
	result := service.NewWorkResult("op-name")
	result.Finish()
	assert.True(t, result.Finished())
}

func TestResultRuntime(t *testing.T) {
	result := service.NewWorkResult("op-name")
	now := time.Now()
	fiveMinutesAgo := now.Add(-5 * time.Minute)
	result.StartedAt = fiveMinutesAgo
	result.FinishedAt = now
	assert.EqualValues(t, 5*time.Minute, result.RunTime())
}

func TestResultSucceeded(t *testing.T) {
	result := service.NewWorkResult("op-name")

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
	result := service.NewWorkResult("op-name")
	result.AddError(nonFatalErr)
	assert.Equal(t, 1, len(result.Errors))
	result.AddError(nonFatalErr)
	assert.Equal(t, 2, len(result.Errors))
}

func TestAddError_Limit(t *testing.T) {
	result := service.NewWorkResult("op-name")
	for i := 0; i < 40; i++ {
		result.AddError(nonFatalErr)
	}
	assert.Equal(t, len(result.Errors), 30)
}

func TestHasErrors(t *testing.T) {
	result := service.NewWorkResult("op-name")
	assert.False(t, result.HasErrors())
	result.AddError(nonFatalErr)
	assert.True(t, result.HasErrors())
}

func TestClearErrors(t *testing.T) {
	result := service.NewWorkResult("op-name")
	result.AddError(fatalErr)
	assert.True(t, result.HasFatalErrors())
	assert.NotEmpty(t, result.Errors)
	result.ClearErrors()
	assert.Empty(t, result.Errors)
	assert.False(t, result.HasFatalErrors())
}

func TestReset(t *testing.T) {
	result := service.NewWorkResult("op-name")
	result.Attempt = 6
	result.Host = "Charlie Sheen"
	result.Pid = 999
	result.StartedAt = testutil.Bloomsday
	result.FinishedAt = testutil.Bloomsday

	result.AddError(fatalErr)
	assert.NotEmpty(t, result.Errors)
	result.Reset()
	assert.Empty(t, result.Host)
	assert.Equal(t, 0, result.Pid)
	assert.Empty(t, result.Errors)
	assert.True(t, result.StartedAt.IsZero())
	assert.True(t, result.FinishedAt.IsZero())
	assert.Equal(t, 6, result.Attempt)
	assert.Equal(t, "op-name", result.Operation)
}

func TestFatalErrors(t *testing.T) {
	result := service.NewWorkResult("op-name")

	result.AddError(nonFatalErr)
	assert.Equal(t, 1, len(result.Errors))
	assert.False(t, result.HasFatalErrors())

	result.AddError(fatalErr)
	assert.True(t, result.HasFatalErrors())

	result.AddError(nonFatalErr)
	result.AddError(fatalErr)

	fatalErrors := result.FatalErrors()
	assert.Equal(t, 2, len(fatalErrors))
	for _, err := range fatalErrors {
		assert.True(t, err.IsFatal)
	}
}

func TestFatalErrorMessage(t *testing.T) {
	result := service.NewWorkResult("op-name")
	result.AddError(fatalErr)
	result.AddError(fatalErr)
	assert.Equal(t, "Fatal error | Fatal error", result.FatalErrorMessage())
}

func TestWorkResultToJson(t *testing.T) {
	result := service.NewWorkResult(constants.IngestPreFetch)
	result.Attempt = 4
	result.Host = "Bogus-Host-Name"
	result.Pid = 1234
	result.StartedAt = testutil.Bloomsday
	result.FinishedAt = testutil.Bloomsday
	jsonData, err := result.ToJSON()
	require.Nil(t, err)
	assert.Equal(t, expectedJson, jsonData)
}

func TestWorkResultFromJson(t *testing.T) {
	result, err := service.WorkResultFromJSON(expectedJson)
	require.Nil(t, err)
	assert.Equal(t, constants.IngestPreFetch, result.Operation)
	assert.Equal(t, "Bogus-Host-Name", result.Host)
	assert.Equal(t, 1234, result.Pid)
	assert.Equal(t, testutil.Bloomsday, result.StartedAt)
	assert.Equal(t, testutil.Bloomsday, result.FinishedAt)
	assert.Equal(t, 0, len(result.Errors))
}

const expectedJson = `{"attempt":4,"operation":"ingest01_prefetch","host":"Bogus-Host-Name","pid":1234,"started_at":"1904-06-16T15:04:05Z","finished_at":"1904-06-16T15:04:05Z","errors":[]}`
