package service_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func TestNewWorkResult(t *testing.T) {
	hostname, _ := os.Hostname()
	result := service.NewWorkResult(constants.OpIngestGatherMeta)
	assert.Equal(t, constants.OpIngestGatherMeta, result.Operation)
	assert.Equal(t, hostname, result.Host)
	assert.Equal(t, os.Getpid(), result.Pid)
	assert.NotNil(t, result.Errors)
	assert.Equal(t, 0, len(result.Errors))
	assert.Equal(t, constants.StatusPending, result.Status)
}

func TestWorkResultAddError(t *testing.T) {
	result := service.NewWorkResult(constants.OpIngestGatherMeta)

	result.AddError("err1", false)
	assert.Equal(t, "err1", result.Errors[0])
	assert.False(t, result.ErrorIsFatal)

	result.AddError("err2", true)
	assert.Equal(t, "err2", result.Errors[1])
	assert.True(t, result.ErrorIsFatal)

	// Fatal errors should not be unset
	result.AddError("err3", false)
	assert.Equal(t, "err3", result.Errors[2])
	assert.True(t, result.ErrorIsFatal)
}

func TestWorkResultStart(t *testing.T) {
	result := service.NewWorkResult(constants.OpIngestGatherMeta)
	assert.True(t, result.StartedAt.IsZero())
	assert.Equal(t, constants.StatusPending, result.Status)

	result.Start()
	assert.False(t, result.StartedAt.IsZero())
	assert.Equal(t, constants.StatusStarted, result.Status)
}
func TestWorkResultFinishWithSuccess(t *testing.T) {
	result := service.NewWorkResult(constants.OpIngestGatherMeta)
	assert.True(t, result.FinishedAt.IsZero())
	assert.Equal(t, constants.StatusPending, result.Status)

	result.FinishWithSuccess()
	assert.False(t, result.FinishedAt.IsZero())
	assert.Equal(t, constants.StatusSuccess, result.Status)
}
func TestWorkResultFinishWithError(t *testing.T) {
	result := service.NewWorkResult(constants.OpIngestGatherMeta)
	assert.True(t, result.FinishedAt.IsZero())
	assert.Equal(t, constants.StatusPending, result.Status)

	result.FinishWithError("oops", false)
	assert.False(t, result.FinishedAt.IsZero())
	assert.Equal(t, constants.StatusFailed, result.Status)
	assert.Equal(t, "oops", result.Errors[0])
	assert.False(t, result.ErrorIsFatal)

	result.Reset()

	result.FinishWithError("fatal oops", true)
	assert.False(t, result.FinishedAt.IsZero())
	assert.Equal(t, constants.StatusFailed, result.Status)
	assert.Equal(t, "fatal oops", result.Errors[0])
	assert.True(t, result.ErrorIsFatal)
}
func TestWorkResultReset(t *testing.T) {
	result := service.NewWorkResult(constants.OpIngestGatherMeta)
	result.FinishWithError("fatal oops", true)
	assert.False(t, result.FinishedAt.IsZero())
	assert.Equal(t, constants.StatusFailed, result.Status)
	assert.Equal(t, "fatal oops", result.Errors[0])
	assert.True(t, result.ErrorIsFatal)

	result.Reset()

	assert.True(t, result.StartedAt.IsZero())
	assert.True(t, result.FinishedAt.IsZero())
	assert.False(t, result.ErrorIsFatal)
	assert.Equal(t, constants.StatusPending, result.Status)
	assert.Equal(t, 0, len(result.Errors))
}

func TestWorkResultToJson(t *testing.T) {
	result := service.NewWorkResult(constants.OpIngestGatherMeta)
	result.Host = "Bogus-Host-Name"
	result.Pid = 1234
	result.Start()
	result.FinishWithError("fatal oops", true)
	result.StartedAt = timestamp
	result.FinishedAt = timestamp
	jsonData, err := result.ToJson()
	require.Nil(t, err)
	assert.Equal(t, expectedJson, jsonData)
}

func TestWorkResultFromJson(t *testing.T) {
	result, err := service.WorkResultFromJson(expectedJson)
	require.Nil(t, err)
	assert.Equal(t, constants.OpIngestGatherMeta, result.Operation)
	assert.Equal(t, "Bogus-Host-Name", result.Host)
	assert.Equal(t, 1234, result.Pid)
	assert.Equal(t, timestamp, result.StartedAt)
	assert.Equal(t, timestamp, result.FinishedAt)
	assert.Equal(t, 1, len(result.Errors))
	assert.Equal(t, "fatal oops", result.Errors[0])
	assert.True(t, result.ErrorIsFatal)
	assert.Equal(t, constants.StatusFailed, result.Status)
}

var timestamp, _ = time.Parse(time.RFC3339, "2020-01-23T21:48:18.07503Z")

const expectedJson = `{"operation":"Ingest - Gather Metadata","host":"Bogus-Host-Name","pid":1234,"attempt_number":1,"started_at":"2020-01-23T21:48:18.07503Z","finished_at":"2020-01-23T21:48:18.07503Z","errors":["fatal oops"],"error_is_fatal":true,"status":"Failed"}`
