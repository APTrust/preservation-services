// +build integration

package deletion_test

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/deletion"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/minio/minio-go/v6"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var objIdentifier = "institution2.edu/springfield"
var fileNames = []string{
	"doc1",
	"doc2",
	"doc3",
}
var alreadySaved = make([]string, 0)
var objectCreated = false

func TestNewManager(t *testing.T) {
	context := common.NewContext()
	manager := deletion.NewManager(
		context,
		9999,
		"test.edu/my_object",
		constants.TypeObject,
		"requestor@example.com",
		"approver@example.com",
		"some-admin@aptrust.org",
	)
	assert.NotNil(t, manager)
	assert.Equal(t, context, manager.Context)
	assert.Equal(t, 9999, manager.WorkItemID)
	assert.Equal(t, "test.edu/my_object", manager.Identifier)
	assert.Equal(t, constants.TypeObject, manager.ItemType)
	assert.Equal(t, "requestor@example.com", manager.RequestedBy)
	assert.Equal(t, "approver@example.com", manager.InstApprover)
	assert.Equal(t, "some-admin@aptrust.org", manager.APTrustApprover)
}

func TestRun_SingleFile(t *testing.T) {
	context := common.NewContext()
	prepareForTest(t, context)
	fileIdentifier := "institution2.edu/springfield/doc1"
	manager := deletion.NewManager(
		context,
		9999,
		fileIdentifier,
		constants.TypeFile,
		"requestor@example.com",
		"approver@example.com",
		"some-admin@aptrust.org",
	)
	count, errors := manager.Run()
	assert.Equal(t, 1, count)
	assert.Empty(t, errors)

	testItemMarkedDeleted(t, context, fileIdentifier)
	testStorageRecordsRemoved(t, context, fileIdentifier)
	testFileDeletionEvents(t, context, fileIdentifier)
}

func TestRun_Object(t *testing.T) {
	context := common.NewContext()
	prepareForTest(t, context)
	itemID := createDeletionWorkItem(t, context, objIdentifier)
	manager := deletion.NewManager(
		context,
		itemID,
		objIdentifier,
		constants.TypeObject,
		"requestor@example.com",
		"approver@example.com",
		"some-admin@aptrust.org",
	)

	// TestRun_SingleFile deletes one of this object's file.
	// This test deletes the other two, so we should get a
	// count of two here.
	count, errors := manager.Run()
	assert.Equal(t, 2, count)
	assert.Empty(t, errors)

	// File 2
	testItemMarkedDeleted(t, context, "institution2.edu/springfield/doc2")
	testStorageRecordsRemoved(t, context, "institution2.edu/springfield/doc2")
	testFileDeletionEvents(t, context, "institution2.edu/springfield/doc2")

	// File 3
	testItemMarkedDeleted(t, context, "institution2.edu/springfield/doc3")
	testStorageRecordsRemoved(t, context, "institution2.edu/springfield/doc3")
	testFileDeletionEvents(t, context, "institution2.edu/springfield/doc3")

	// IntellectualObject
	testItemMarkedDeleted(t, context, "institution2.edu/springfield")
	testObjectDeletionEvent(t, context)
}

func testItemMarkedDeleted(t *testing.T, context *common.Context, identifier string) {
	if identifier == objIdentifier {
		resp := context.PharosClient.IntellectualObjectGet(identifier)
		require.Nil(t, resp.Error)
		assert.Equal(t, constants.StateDeleted, resp.IntellectualObject().State)
	} else {
		resp := context.PharosClient.GenericFileGet(identifier)
		require.Nil(t, resp.Error)
		assert.Equal(t, constants.StateDeleted, resp.GenericFile().State)
	}
}

func testStorageRecordsRemoved(t *testing.T, context *common.Context, gfIdentifier string) {
	resp := context.PharosClient.StorageRecordList(gfIdentifier)
	require.Nil(t, resp.Error)
	assert.Equal(t, 0, len(resp.StorageRecords()))
}

func testFileDeletionEvents(t *testing.T, context *common.Context, gfIdentifier string) {
	instId := getInstId(t, context)
	values := url.Values{}
	values.Set("file_identifier", gfIdentifier)
	values.Set("event_type", constants.EventDeletion)
	values.Set("page", "1")
	values.Set("per_page", "20")
	resp := context.PharosClient.PremisEventList(values)
	require.Nil(t, resp.Error)
	deletionEvents := resp.PremisEvents()
	assert.Equal(t, 10, len(deletionEvents))
	for _, event := range deletionEvents {
		assert.True(t, strings.Contains(event.OutcomeInformation, "requestor@example.com"))
		assert.True(t, strings.Contains(event.OutcomeInformation, "approver@example.com"))
		assert.True(t, strings.Contains(event.OutcomeInformation, "some-admin@aptrust.org"))
		assert.True(t, strings.Contains(event.Detail, "Deleted one copy of this file from"))
		assert.True(t, strings.Contains(event.Detail, "localhost:9899"))
		assert.Equal(t, constants.StatusSuccess, event.Outcome)
		assert.Equal(t, objIdentifier, event.IntellectualObjectIdentifier)
		assert.NotEqual(t, 0, event.IntellectualObjectID)
		assert.Equal(t, instId, event.InstitutionID)
	}
}

func testObjectDeletionEvent(t *testing.T, context *common.Context) {
	values := url.Values{}
	values.Set("object_identifier", objIdentifier)
	values.Set("file_identifier", "")
	values.Set("event_type", constants.EventDeletion)
	values.Set("page", "1")
	values.Set("per_page", "100")
	resp := context.PharosClient.PremisEventList(values)
	require.Nil(t, resp.Error)
	// Pharos doesn't filter these results properly
	count := 0
	for _, event := range resp.PremisEvents() {
		if event.GenericFileIdentifier == "" {
			count++
		}
	}
	assert.Equal(t, 1, count)
}

func prepareForTest(t *testing.T, context *common.Context) {
	createObjectAndFiles(t, context)
	copyFilesToLocalPreservation(t, context)
}

// Create a new intellectual object that we can delete.
// To make things simple, we copy an existing object, changing
// the name, ID, and identifier.
func createObjectAndFiles(t *testing.T, context *common.Context) {
	if !objectCreated {
		resp := context.PharosClient.IntellectualObjectGet("institution2.edu/coal")
		require.Nil(t, resp.Error)
		obj := resp.IntellectualObject()
		require.NotNil(t, obj)
		obj.ID = 0
		obj.Identifier = objIdentifier
		obj.BagName = "springfield.tar"
		obj.State = constants.StateActive
		resp = context.PharosClient.IntellectualObjectSave(obj)
		require.Nil(t, resp.Error)
		savedObj := resp.IntellectualObject()

		for _, file := range fileNames {
			gf := &registry.GenericFile{
				FileFormat:                   "application/ms-word",
				Identifier:                   fmt.Sprintf("%s/%s", objIdentifier, file),
				InstitutionID:                savedObj.InstitutionID,
				IntellectualObjectID:         savedObj.ID,
				IntellectualObjectIdentifier: savedObj.Identifier,
				Size:                         500,
				State:                        "A",
				StorageOption:                constants.StorageStandard,
				URI:                          "http://localhost/fake-uri",
			}
			resp = context.PharosClient.GenericFileSave(gf)
			require.Nil(t, resp.Error)
			ingestEvent := getFileIngestEvent(gf)
			resp = context.PharosClient.PremisEventSave(ingestEvent)
			require.Nil(t, resp.Error)
		}
	}
	objectCreated = true
}

func copyFilesToLocalPreservation(t *testing.T, context *common.Context) {
	for _, filename := range fileNames {
		copyFileToBuckets(t, context, filename)
	}
}

// This copies a file into each of the preservation buckets. Note that we
// copy the same file every time. We just give it a different key name in
// the preservation bucket. For the puposes of our test, all we care about
// is whether the files are deleted by the end.
func copyFileToBuckets(t *testing.T, context *common.Context, filename string) {
	pathToFile := testutil.PathToUnitTestBag("example.edu.multipart.b01.of02.tar")
	gfIdentifier := fmt.Sprintf("%s/%s", objIdentifier, filename)
	for _, preservationBucket := range context.Config.PerservationBuckets {
		_url := preservationBucket.URLFor(filename)
		if util.StringListContains(alreadySaved, _url) {
			continue
		}
		client := context.S3Clients[preservationBucket.Provider]
		_, err := client.FPutObject(
			preservationBucket.Bucket,
			filename,
			pathToFile,
			minio.PutObjectOptions{},
		)
		require.Nil(t, err)

		storageRecord := &registry.StorageRecord{
			URL: _url,
		}
		resp := context.PharosClient.StorageRecordSave(storageRecord, gfIdentifier)
		require.Nil(t, resp.Error)
		alreadySaved = append(alreadySaved, _url)
	}
}

// We have to create a deletion WorkItem for this object,
// or Pharos returns the following error when we call the
// object's finish_delete endpoint:
// "There is no existing deletion request for the specified object."
func createDeletionWorkItem(t *testing.T, context *common.Context, identifier string) int {
	now := time.Now().UTC()
	gfIdentifier := ""
	if identifier != objIdentifier {
		gfIdentifier = identifier
	}
	item := &registry.WorkItem{
		APTrustApprover:       "some-admin@aptrust.org",
		Action:                constants.ActionDelete,
		BagDate:               testutil.Bloomsday,
		Bucket:                "receiving",
		CreatedAt:             now,
		Date:                  now,
		ETag:                  "1234",
		GenericFileIdentifier: gfIdentifier,
		InstApprover:          "approver@example.com",
		InstitutionID:         getInstId(t, context),
		Name:                  "springfield.tar",
		NeedsAdminReview:      false,
		Note:                  "Deletion requested",
		ObjectIdentifier:      objIdentifier,
		Outcome:               "Deleteion requested",
		QueuedAt:              now,
		Retry:                 true,
		Size:                  500,
		Stage:                 constants.StageRequested,
		Status:                constants.StatusPending,
		UpdatedAt:             now,
		User:                  "requestor@example.com",
	}
	resp := context.PharosClient.WorkItemSave(item)
	require.Nil(t, resp.Error)
	return resp.WorkItem().ID
}

func getInstId(t *testing.T, context *common.Context) int {
	resp := context.PharosClient.InstitutionGet("institution2.edu")
	require.Nil(t, resp.Error)
	return resp.Institution().ID
}

// Sigh... Pharos internal logic requires that this record exist
// before it will allow a deletion to be marked complete.
func getFileIngestEvent(gf *registry.GenericFile) *registry.PremisEvent {
	eventId := uuid.NewV4()
	timestamp := time.Now().UTC().Add(-1 * time.Minute)
	return &registry.PremisEvent{
		Identifier:                   eventId.String(),
		EventType:                    constants.EventIngestion,
		DateTime:                     timestamp,
		Detail:                       fmt.Sprintf("Item was ingested"),
		Outcome:                      constants.StatusSuccess,
		OutcomeDetail:                "yadda yadda yadda",
		Object:                       "preservation-services + Minio S3 client",
		Agent:                        constants.S3ClientName,
		OutcomeInformation:           "blah blah blah",
		IntellectualObjectIdentifier: gf.IntellectualObjectIdentifier,
		GenericFileIdentifier:        gf.Identifier,
		InstitutionID:                gf.InstitutionID,
		IntellectualObjectID:         gf.IntellectualObjectID,
		CreatedAt:                    timestamp,
		UpdatedAt:                    timestamp,
	}
}
