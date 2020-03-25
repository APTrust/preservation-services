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
	Date:                  testutil.Bloomsday,
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

var itemJson = `{"aptrust_approver":"nobody@aptrust.org","action":"Ingest","bag_date":"1904-06-16T15:04:05Z","bucket":"receiving-bucket","created_at":"1904-06-16T15:04:05Z","date":"1904-06-16T15:04:05Z","etag":"54321","generic_file_identifier":"test.edu/bag/data/file.txt","id":908,"inst_appropver":"admin@test.edu","institution_id":333,"name":"test.edu.bag.tar","needs_admin_review":false,"node":"apt-prod-services-01","note":"This is just to say...","object_identifier":"test.edu/bag","outcome":"Ingest in progress","pid":3100,"queued_at":"1904-06-16T15:04:05Z","retry":true,"size":2858933,"stage":"Receive","stage_started_at":"1904-06-16T15:04:05Z","status":"Started","updated_at":"1904-06-16T15:04:05Z","user":"user@example.com"}`

var itemJsonForPharos = `{"aptrust_approver":"nobody@aptrust.org","action":"Ingest","bag_date":"1904-06-16T15:04:05Z","bucket":"receiving-bucket","date":"1904-06-16T15:04:05Z","etag":"54321","generic_file_identifier":"test.edu/bag/data/file.txt","inst_appropver":"admin@test.edu","institution_id":333,"name":"test.edu.bag.tar","needs_admin_review":false,"node":"apt-prod-services-01","note":"This is just to say...","object_identifier":"test.edu/bag","outcome":"Ingest in progress","pid":3100,"queued_at":"1904-06-16T15:04:05Z","retry":true,"size":2858933,"stage":"Receive","stage_started_at":"1904-06-16T15:04:05Z","status":"Started","user":"user@example.com"}`

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

func TestWorkItemSerializeForPharos(t *testing.T) {
	actualJson, err := item.SerializeForPharos()
	require.Nil(t, err)
	assert.Equal(t, itemJsonForPharos, string(actualJson))
}
