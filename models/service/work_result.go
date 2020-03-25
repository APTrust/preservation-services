package service

import (
	"encoding/json"
	"os"
	"time"

	"github.com/APTrust/preservation-services/constants"
)

type WorkResult struct {
	Operation     string    `json:"operation"`
	Host          string    `json:"host"`
	Pid           int       `json:"pid"`
	AttemptNumber int       `json:"attempt_number"`
	StartedAt     time.Time `json:"started_at"`
	FinishedAt    time.Time `json:"finished_at"`
	Errors        []string  `json:"errors"`
	ErrorIsFatal  bool      `json:"error_is_fatal"`
	Status        string    `json:"status"`
}

func NewWorkResult(operation string) *WorkResult {
	hostname, _ := os.Hostname()
	return &WorkResult{
		Operation: operation,
		Host:      hostname,
		Pid:       os.Getpid(),
		Errors:    make([]string, 0),
		Status:    constants.StatusPending,
	}
}

func (result *WorkResult) AddError(message string, isFatal bool) {
	result.Errors = append(result.Errors, message)
	if isFatal {
		result.ErrorIsFatal = true
		result.Status = constants.StatusFailed
	}
}

func (result *WorkResult) Start() {
	result.AttemptNumber += 1
	result.Status = constants.StatusStarted
	result.StartedAt = time.Now().UTC()
}

func (result *WorkResult) FinishWithSuccess() {
	result.Status = constants.StatusSuccess
	result.FinishedAt = time.Now().UTC()
}

func (result *WorkResult) FinishWithError(message string, isFatal bool) {
	result.Status = constants.StatusFailed
	result.FinishedAt = time.Now().UTC()
	result.AddError(message, isFatal)
}

func (result *WorkResult) Reset() {
	result.ErrorIsFatal = false
	result.Status = constants.StatusPending
	result.Errors = make([]string, 0)
	result.StartedAt = time.Time{}
	result.FinishedAt = time.Time{}
}

func WorkResultFromJSON(jsonData string) (*WorkResult, error) {
	result := &WorkResult{}
	err := json.Unmarshal([]byte(jsonData), result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (result *WorkResult) ToJSON() (string, error) {
	bytes, err := json.Marshal(result)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
