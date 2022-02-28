//go:build integration
// +build integration

package deletion_test

import (
	ctx "context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/deletion"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var objIdentifier = "institution2.edu/springfield"
var instID = int64(3)
var fileNames = []string{
	"doc1",
	"doc2",
	"doc3",
}
var alreadySaved = make([]string, 0)

// The following are all set the first time createObjectAndFiles is called.
// This was retrofitted after the creation of the Registry.
var objectCreated = false
var savedObj *registry.IntellectualObject
var savedFiles []*registry.GenericFile

func TestNewManager(t *testing.T) {
	context := common.NewContext()
	manager := deletion.NewManager(
		context,
		9999,
		12345,
		constants.TypeObject,
		"requestor@example.com",
		"approver@example.com",
		"some-admin@aptrust.org",
	)
	assert.NotNil(t, manager)
	assert.Equal(t, context, manager.Context)
	assert.EqualValues(t, 9999, manager.WorkItemID)
	assert.EqualValues(t, 12345, manager.ObjOrFileID)
	assert.Equal(t, constants.TypeObject, manager.ItemType)
	assert.Equal(t, "requestor@example.com", manager.RequestedBy)
	assert.Equal(t, "approver@example.com", manager.InstApprover)
	assert.Equal(t, "some-admin@aptrust.org", manager.APTrustApprover)
}

func TestRun_SingleFile(t *testing.T) {
	context := common.NewContext()
	prepareForTest(t, context)
	gf := savedFiles[0] // doc1

	resp := context.RegistryClient.GenericFilePrepareForDelete(gf.ID)
	require.Nil(t, resp.Error)
	workItem := resp.WorkItem()
	require.True(t, workItem.ID > 0)

	manager := deletion.NewManager(
		context,
		workItem.ID,
		gf.ID,
		constants.TypeFile,
		"requestor@example.com",
		"approver@example.com",
		"some-admin@aptrust.org",
	)
	count, errors := manager.Run()
	assert.Equal(t, 1, count)
	assert.Empty(t, errors)

	testItemMarkedDeleted(t, context, constants.TypeFile, gf.ID)
	testStorageRecordsRemoved(t, context, gf.ID)
	testFileDeletionEvents(t, context, gf.ID)
}

func TestRun_Object(t *testing.T) {
	context := common.NewContext()
	prepareForTest(t, context)

	resp := context.RegistryClient.IntellectualObjectPrepareForDelete(savedObj.ID)
	require.Nil(t, resp.Error)
	workItem := resp.WorkItem()
	require.True(t, workItem.ID > 0)

	manager := deletion.NewManager(
		context,
		workItem.ID,
		savedObj.ID,
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
	testItemMarkedDeleted(t, context, constants.TypeFile, savedFiles[1].ID)
	testStorageRecordsRemoved(t, context, savedFiles[1].ID)
	testFileDeletionEvents(t, context, savedFiles[1].ID)

	// File 3
	testItemMarkedDeleted(t, context, constants.TypeFile, savedFiles[2].ID)
	testStorageRecordsRemoved(t, context, savedFiles[2].ID)
	testFileDeletionEvents(t, context, savedFiles[2].ID)

	// IntellectualObject
	testItemMarkedDeleted(t, context, constants.TypeObject, savedObj.ID)
	testObjectDeletionEvent(t, context)
}

func testItemMarkedDeleted(t *testing.T, context *common.Context, itemType string, itemID int64) {
	if itemType == constants.TypeObject {
		resp := context.RegistryClient.IntellectualObjectByID(itemID)
		require.Nil(t, resp.Error)
		assert.Equal(t, constants.StateDeleted, resp.IntellectualObject().State)
	} else {
		resp := context.RegistryClient.GenericFileByID(itemID)
		require.Nil(t, resp.Error)
		assert.Equal(t, constants.StateDeleted, resp.GenericFile().State)
	}
}

func testStorageRecordsRemoved(t *testing.T, context *common.Context, gfID int64) {
	values := url.Values{}
	values.Add("generic_file_id", strconv.FormatInt(gfID, 10))
	resp := context.RegistryClient.StorageRecordList(values)
	require.Nil(t, resp.Error)
	assert.Equal(t, 0, len(resp.StorageRecords()))
}

func testFileDeletionEvents(t *testing.T, context *common.Context, gfID int64) {
	instId := getInstId(t, context)
	values := url.Values{}
	values.Set("generic_file_id", strconv.FormatInt(gfID, 10))
	values.Set("event_type", constants.EventDeletion)
	values.Set("page", "1")
	values.Set("per_page", "20")
	resp := context.RegistryClient.PremisEventList(values)
	require.Nil(t, resp.Error)
	deletionEvents := resp.PremisEvents()
	// One event for each copy deletion, plus one signalling
	// that all copies have been deleted.
	assert.Equal(t, 11, len(deletionEvents))
	for _, event := range deletionEvents {
		// Deletion manager and Registry internal log slighly different messages.
		// The second is for Registry internal, indicating deletion of all copies.
		assert.True(t, (event.OutcomeInformation == "File deleted at the request of requestor@example.com. Institutional approver: approver@example.com. APTrust approver: some-admin@aptrust.org." || event.OutcomeInformation == "File deleted at the request of admin@inst2.edu. Institutional approver: admin@inst2.edu."))
		assert.True(t, (strings.HasPrefix(event.Detail, "Deleted one copy of this file from") || event.Detail == "File deleted from preservation storage"), event.Detail)
		assert.Equal(t, constants.StatusSuccess, event.Outcome)
		assert.Equal(t, objIdentifier, event.IntellectualObjectIdentifier)
		assert.NotEqual(t, 0, event.IntellectualObjectID)
		assert.Equal(t, instId, event.InstitutionID)
	}
}

func testObjectDeletionEvent(t *testing.T, context *common.Context) {
	values := url.Values{}
	values.Set("intellectual_object_id", strconv.FormatInt(savedObj.ID, 10))
	values.Set("event_type", constants.EventDeletion)
	values.Set("page", "1")
	values.Set("per_page", "100")
	resp := context.RegistryClient.PremisEventList(values)
	require.Nil(t, resp.Error)

	// There should be one deletion event for the object,
	// and eleven for each file (one for each of the ten copies
	// of the file, which we copied to all ten buckets, and one
	// for the overall deletion).
	objCount := 0
	fileCount := 0
	for _, event := range resp.PremisEvents() {
		if event.GenericFileID == 0 {
			objCount++
		} else {
			fileCount++
		}
	}
	assert.Equal(t, 1, objCount)
	assert.Equal(t, 33, fileCount)
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
		resp := context.RegistryClient.IntellectualObjectByIdentifier("institution2.edu/coal")
		require.Nil(t, resp.Error)
		obj := resp.IntellectualObject()
		require.NotNil(t, obj)
		obj.ID = 0
		obj.Identifier = objIdentifier
		obj.BagName = "springfield.tar"
		obj.State = constants.StateActive
		resp = context.RegistryClient.IntellectualObjectSave(obj)
		require.Nil(t, resp.Error)
		savedObj = resp.IntellectualObject()
		savedFiles = make([]*registry.GenericFile, len(fileNames))

		for i, file := range fileNames {
			gf := &registry.GenericFile{
				FileFormat:           "application/ms-word",
				Identifier:           fmt.Sprintf("%s/%s", objIdentifier, file),
				InstitutionID:        savedObj.InstitutionID,
				IntellectualObjectID: savedObj.ID,
				Size:                 500,
				State:                "A",
				StorageOption:        constants.StorageStandard,
				UUID:                 uuid.New().String(),
			}
			resp = context.RegistryClient.GenericFileSave(gf)
			require.Nil(t, resp.Error)
			savedFiles[i] = resp.GenericFile()
			ingestEvent := getFileIngestEvent(gf)
			resp = context.RegistryClient.PremisEventSave(ingestEvent)
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

	resp := context.RegistryClient.GenericFileByIdentifier(gfIdentifier)
	require.Nil(t, resp.Error)
	gf := resp.GenericFile()
	require.NotNil(t, gf)

	for _, preservationBucket := range context.Config.PreservationBuckets {
		_url := preservationBucket.URLFor(filename)
		if util.StringListContains(alreadySaved, _url) {
			continue
		}
		client := context.S3Clients[preservationBucket.Provider]
		_, err := client.FPutObject(
			ctx.Background(),
			preservationBucket.Bucket,
			filename,
			pathToFile,
			minio.PutObjectOptions{},
		)
		require.Nil(t, err)

		storageRecord := &registry.StorageRecord{
			URL:           _url,
			GenericFileID: gf.ID,
		}
		resp := context.RegistryClient.StorageRecordCreate(storageRecord, gf.InstitutionID)
		require.Nil(t, resp.Error)
		alreadySaved = append(alreadySaved, _url)
	}
}

func getInstId(t *testing.T, context *common.Context) int64 {
	resp := context.RegistryClient.InstitutionByIdentifier("institution2.edu")
	require.Nil(t, resp.Error)
	return resp.Institution().ID
}

// Registry internal logic requires that this record exist
// before it will allow a deletion to be marked complete.
func getFileIngestEvent(gf *registry.GenericFile) *registry.PremisEvent {
	eventId := uuid.New()
	timestamp := time.Now().UTC().Add(-1 * time.Minute)
	return &registry.PremisEvent{
		Identifier:           eventId.String(),
		EventType:            constants.EventIngestion,
		DateTime:             timestamp,
		Detail:               fmt.Sprintf("Item was ingested"),
		Outcome:              constants.StatusSuccess,
		OutcomeDetail:        "yadda yadda yadda",
		Object:               "preservation-services + Minio S3 client",
		Agent:                constants.S3ClientName,
		OutcomeInformation:   "blah blah blah",
		InstitutionID:        gf.InstitutionID,
		IntellectualObjectID: gf.IntellectualObjectID,
		CreatedAt:            timestamp,
		UpdatedAt:            timestamp,
	}
}
