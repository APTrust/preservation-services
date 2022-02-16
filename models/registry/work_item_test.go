package registry_test

import (
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var item = &registry.WorkItem{
	APTrustApprover:       "nobody@aptrust.org",
	Action:                constants.ActionIngest,
	BagDate:               testutil.Bloomsday,
	Bucket:                "receiving-bucket",
	CreatedAt:             testutil.Bloomsday,
	DateProcessed:         testutil.Bloomsday,
	ETag:                  "54321",
	GenericFileIdentifier: "test.edu/bag/data/file.txt",
	ID:                    908,
	InstApprover:          "admin@test.edu",
	InstitutionID:         333,
	Name:                  "test.edu.bag.tar",
	NeedsAdminReview:      false,
	Node:                  "apt-prod-services-01",
	Note:                  "This is just to say...",
	ObjectIdentifier:      "test.edu/bag",
	Outcome:               "Ingest in progress",
	Pid:                   3100,
	QueuedAt:              testutil.Bloomsday,
	Retry:                 true,
	Size:                  int64(2858933),
	Stage:                 constants.StageReceive,
	StageStartedAt:        testutil.Bloomsday,
	Status:                constants.StatusStarted,
	UpdatedAt:             testutil.Bloomsday,
	User:                  "user@example.com",
}

var itemJson = `{"aptrust_approver":"nobody@aptrust.org","action":"Ingest","bag_date":"1904-06-16T15:04:05Z","bucket":"receiving-bucket","created_at":"1904-06-16T15:04:05Z","date":"1904-06-16T15:04:05Z","etag":"54321","generic_file_identifier":"test.edu/bag/data/file.txt","id":908,"inst_approver":"admin@test.edu","institution_id":333,"name":"test.edu.bag.tar","needs_admin_review":false,"node":"apt-prod-services-01","note":"This is just to say...","object_identifier":"test.edu/bag","outcome":"Ingest in progress","pid":3100,"queued_at":"1904-06-16T15:04:05Z","retry":true,"size":2858933,"stage":"Receive","stage_started_at":"1904-06-16T15:04:05Z","status":"Started","updated_at":"1904-06-16T15:04:05Z","user":"user@example.com"}`

var itemWithNullQueuedAt = `{"aptrust_approver":"nobody@aptrust.org","action":"Ingest","bag_date":"1904-06-16T15:04:05Z","bucket":"receiving-bucket","date":"1904-06-16T15:04:05Z","etag":"54321","generic_file_identifier":"test.edu/bag/data/file.txt","inst_approver":"admin@test.edu","institution_id":333,"name":"test.edu.bag.tar","needs_admin_review":false,"node":"apt-prod-services-01","note":"This is just to say...","object_identifier":"test.edu/bag","outcome":"Ingest in progress","pid":3100,"queued_at":null,"retry":true,"size":2858933,"stage":"Receive","stage_started_at":"1904-06-16T15:04:05Z","status":"Started","user":"user@example.com"}`

func TestWorkItemFromJson(t *testing.T) {
	workItem, err := registry.WorkItemFromJSON([]byte(itemJson))
	require.Nil(t, err)
	assert.Equal(t, item, workItem)
}

func TestWorkItemToJson(t *testing.T) {
	actualJson, err := item.ToJSON()
	require.Nil(t, err)
	assert.Equal(t, itemJson, string(actualJson))
}

func TestWorkItemProcessingHasCompleted(t *testing.T) {
	for _, status := range constants.CompletedStatusValues {
		item := &registry.WorkItem{
			Status: status,
		}
		assert.True(t, item.ProcessingHasCompleted())
	}
	for _, status := range constants.IncompleteStatusValues {
		item := &registry.WorkItem{
			Status: status,
		}
		assert.False(t, item.ProcessingHasCompleted())
	}
}

func TestWorkItemSetNodeAndPid(t *testing.T) {
	item := &registry.WorkItem{}
	assert.Equal(t, "", item.Node)
	assert.Equal(t, 0, item.Pid)
	item.SetNodeAndPid()
	assert.NotEqual(t, "", item.Node)
	assert.NotEqual(t, 0, item.Pid)
}

func TestWorkItemClearNodeAndPid(t *testing.T) {
	item := &registry.WorkItem{
		Node: "worker.aptrust.org",
		Pid:  1234,
	}
	assert.Equal(t, "worker.aptrust.org", item.Node)
	assert.Equal(t, 1234, item.Pid)
	item.ClearNodeAndPid()
	assert.Equal(t, "", item.Node)
	assert.Equal(t, 0, item.Pid)
}

func TestWorkItemMarkInProgress(t *testing.T) {
	item := &registry.WorkItem{}
	item.MarkInProgress(
		constants.StageRecord,
		constants.StatusStarted,
		"Recording ingest metadata in Registry",
	)
	// Not this also sets node and pid
	assert.NotEqual(t, "", item.Node)
	assert.NotEqual(t, 0, item.Pid)
	assert.Equal(t, constants.StageRecord, item.Stage)
	assert.Equal(t, constants.StatusStarted, item.Status)
	assert.Equal(t, "Recording ingest metadata in Registry", item.Note)
	assert.False(t, item.StageStartedAt.IsZero())
}

func TestWorkItemMarkNoLongerInProgress(t *testing.T) {
	item := &registry.WorkItem{
		Node: "worker.aptrust.org",
		Pid:  1234,
	}
	item.MarkNoLongerInProgress(
		constants.StageCleanup,
		constants.StatusPending,
		"Recorded ingest metadata in Registry, awaiting cleanup",
	)
	assert.Equal(t, "", item.Node)
	assert.Equal(t, 0, item.Pid)
	assert.Equal(t, constants.StageCleanup, item.Stage)
	assert.Equal(t, constants.StatusPending, item.Status)
	assert.Equal(t, "Recorded ingest metadata in Registry, awaiting cleanup", item.Note)
}
