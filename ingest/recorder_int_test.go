//go:build integration

package ingest_test

import (
	"net/url"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"

	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const recorderItemID_01 = 32998
const recorderItemID_02 = 32999

func getBagPath(folder, filename string) string {
	return path.Join(testutil.PathToTestData(), "int_test_bags", folder, filename)
}

func TestNewRecorder(t *testing.T) {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	recorder := ingest.NewRecorder(context, 333, obj)
	require.NotNil(t, recorder)
	assert.Equal(t, context, recorder.Context)
	assert.Equal(t, obj, recorder.IngestObject)
	assert.EqualValues(t, 333, recorder.WorkItemID)
}

func TestRecorderRun(t *testing.T) {
	context := common.NewContext()
	bagPath := getBagPath("original", "test.edu.apt-001.tar")
	recorder := prepareForRecord(t, bagPath, recorderItemID_01, context)
	require.NotNil(t, recorder)
	fileCount, errors := recorder.Run()
	require.Empty(t, errors)
	assert.Equal(t, 18, fileCount)

	// Since the last step of ingest succeeded, the original
	// tar file in the receiving bucket should be marked for
	// deletion.
	assert.True(t, recorder.IngestObject.ShouldDeleteFromReceiving)

	testNewObjectInRegistry(t, recorder)
	testNewFilesInRegistry(t, recorder)

	// Now process an update of the same bag and make
	// sure all info is recorded correctly.
	testObjectUpdate(t, context)
}

func testNewObjectInRegistry(t *testing.T, recorder *ingest.Recorder) {
	client := recorder.Context.RegistryClient
	resp := client.IntellectualObjectByIdentifier(recorder.IngestObject.Identifier())
	require.Nil(t, resp.Error)
	intelObj := resp.IntellectualObject()
	require.NotNil(t, intelObj)

	assert.Equal(t, constants.AccessInstitution, intelObj.Access)
	assert.Equal(t, "bag001", intelObj.AltIdentifier)
	assert.Equal(t, "apt-001", intelObj.BagGroupIdentifier)
	assert.Equal(t, "https://raw.githubusercontent.com/APTrust/preservation-services/master/profiles/aptrust-v2.2.json", intelObj.BagItProfileIdentifier)
	assert.Equal(t, "test.edu.apt-001", intelObj.BagName)
	assert.False(t, intelObj.CreatedAt.IsZero())
	assert.Equal(t, "Test bag 001 for integration tests", intelObj.Description)
	assert.Equal(t, 32, len(intelObj.ETag))
	assert.True(t, intelObj.ID > 0)
	assert.Equal(t, "test.edu/test.edu.apt-001", intelObj.Identifier)
	assert.Equal(t, "test.edu", intelObj.InstitutionIdentifier)
	assert.Equal(t, "bag001", intelObj.InternalSenderIdentifier)
	assert.Equal(t, "Test bag 001 for integration tests", intelObj.InternalSenderDescription)
	assert.True(t, intelObj.InstitutionID > 0)
	assert.Equal(t, intelObj.SourceOrganization, "Test University")
	assert.Equal(t, intelObj.State, "A")
	assert.Equal(t, intelObj.StorageOption, constants.StorageClassStandard)
	assert.Equal(t, intelObj.Title, "APTrust Test Bag 001")
	assert.False(t, intelObj.UpdatedAt.IsZero())

	testObjectEventsInRegistry(t, recorder)
}

func testObjectEventsInRegistry(t *testing.T, recorder *ingest.Recorder) {
	client := recorder.Context.RegistryClient
	params := url.Values{}
	params.Add("intellectual_object_id", strconv.FormatInt(recorder.IngestObject.ID, 10))
	params.Add("generic_file_id__is_null", "true")
	params.Add("per_page", "20")
	params.Add("page", "1")

	resp := client.PremisEventList(params)
	require.Nil(t, resp.Error)
	events := resp.PremisEvents()
	require.NotEmpty(t, events)

	eventTypes := make(map[string]int)
	for _, event := range events {
		if _, ok := eventTypes[event.EventType]; !ok {
			eventTypes[event.EventType] = 0
		}
		eventTypes[event.EventType]++
		assert.NotEmpty(t, event.Agent)
		assert.NotEmpty(t, event.DateTime)
		assert.NotEmpty(t, event.Detail)
		assert.NotEmpty(t, event.EventType)

		// No Generic File for object-level events
		assert.EqualValues(t, 0, event.GenericFileID)
		assert.Empty(t, event.GenericFileIdentifier)

		assert.NotEmpty(t, event.Identifier)
		assert.NotEmpty(t, event.InstitutionID)
		assert.NotEmpty(t, event.IntellectualObjectID)
		assert.NotEmpty(t, event.IntellectualObjectIdentifier)
		assert.NotEmpty(t, event.Object)
		assert.NotEmpty(t, event.OutcomeDetail)
		assert.NotEmpty(t, event.OutcomeInformation)
		assert.NotEmpty(t, event.Outcome)
	}
	assert.Equal(t, 1, eventTypes[constants.EventAccessAssignment])
	assert.Equal(t, 1, eventTypes[constants.EventCreation])
	assert.Equal(t, 1, eventTypes[constants.EventIdentifierAssignment])
	assert.Equal(t, 1, eventTypes[constants.EventIngestion])
}

func testNewFilesInRegistry(t *testing.T, recorder *ingest.Recorder) {
	objIdentifier := recorder.IngestObject.Identifier()

	client := recorder.Context.RegistryClient
	params := url.Values{}
	params.Add("intellectual_object_id", strconv.FormatInt(recorder.IngestObject.ID, 10))
	params.Add("per_page", "100")
	params.Add("page", "1")
	resp := client.GenericFileList(params)
	require.Nil(t, resp.Error)
	genericFiles := resp.GenericFiles()
	require.NotEmpty(t, genericFiles)

	// TODO: Save FileModified and StorageRecords

	for _, gf := range genericFiles {
		assert.False(t, gf.CreatedAt.IsZero())
		assert.True(t, strings.Contains(gf.FileFormat, "/"), "%s - %s", gf.Identifier, gf.FileFormat)
		assert.True(t, gf.ID > 0)
		assert.True(t, strings.HasPrefix(gf.Identifier, objIdentifier))
		assert.True(t, len(gf.Identifier) > len(objIdentifier))
		assert.True(t, gf.InstitutionID > 0)
		assert.True(t, gf.IntellectualObjectID > 0)
		gfObjIdentifier, _ := gf.IntellectualObjectIdentifier()
		assert.Equal(t, objIdentifier, gfObjIdentifier)
		assert.False(t, gf.LastFixityCheck.IsZero())
		assert.True(t, gf.Size > 0)
		assert.Equal(t, "A", gf.State)
		assert.Equal(t, constants.StorageStandard, gf.StorageOption)
		assert.True(t, util.LooksLikeUUID(gf.UUID))
		assert.False(t, gf.UpdatedAt.IsZero())

		testFileEventsInRegistry(t, recorder, gf)
		testChecksumsInRegistry(t, recorder, gf)
	}
}

func testFileEventsInRegistry(t *testing.T, recorder *ingest.Recorder, gf *registry.GenericFile) {
	objIdentifier, err := gf.IntellectualObjectIdentifier() //recorder.IngestObject.Identifier()
	require.Nil(t, err)
	client := recorder.Context.RegistryClient
	params := url.Values{}
	params.Add("generic_file_id", strconv.FormatInt(gf.ID, 10))
	params.Add("per_page", "100")
	params.Add("page", "1")

	resp := client.PremisEventList(params)
	require.Nil(t, resp.Error)
	events := resp.PremisEvents()
	require.NotEmpty(t, events)

	eventTypes := make(map[string]int)
	for _, event := range events {
		if _, ok := eventTypes[event.EventType]; !ok {
			eventTypes[event.EventType] = 0
		}
		eventTypes[event.EventType]++
		assert.NotEmpty(t, event.Agent)
		assert.NotEmpty(t, event.DateTime)
		assert.NotEmpty(t, event.Detail)
		assert.NotEmpty(t, event.EventType)
		assert.NotEqual(t, 0, event.GenericFileID)
		assert.Equal(t, gf.Identifier, event.GenericFileIdentifier)
		assert.NotEmpty(t, event.Identifier)
		assert.NotEmpty(t, event.InstitutionID)
		assert.NotEmpty(t, event.IntellectualObjectID, event)
		assert.Equal(t, objIdentifier, event.IntellectualObjectIdentifier, gf.Identifier)
		assert.NotEmpty(t, event.Object)
		assert.NotEmpty(t, event.OutcomeDetail)
		assert.NotEmpty(t, event.OutcomeInformation)
		assert.NotEmpty(t, event.Outcome)
	}

	// md5, sha1, sha256, sha512
	assert.Equal(t, 4, eventTypes[constants.EventDigestCalculation])

	// 1) semantic identifier assignment, 2) URL identifier assignment
	assert.Equal(t, 2, eventTypes[constants.EventIdentifierAssignment])

	assert.Equal(t, 1, eventTypes[constants.EventIngestion])
	assert.Equal(t, 1, eventTypes[constants.EventReplication])
}

func testChecksumsInRegistry(t *testing.T, recorder *ingest.Recorder, gf *registry.GenericFile) {
	params := url.Values{}
	params.Add("generic_file_identifier", gf.Identifier)
	params.Add("per_page", "100")
	params.Add("page", "1")

	resp := recorder.Context.RegistryClient.ChecksumList(params)
	require.Nil(t, resp.Error)
	checksums := resp.Checksums()
	assert.Equal(t, 4, len(checksums))

	for _, gfChecksum := range gf.Checksums {
		found := false
		for _, cs := range checksums {
			if cs.Digest == gfChecksum.Digest {
				found = true
			}
		}
		assert.True(t, found, gfChecksum.Algorithm)
	}
}

// -------------------------------------------------------------------
// Tests for changed/added files in updated bag
// -------------------------------------------------------------------

type ChangedFile struct {
	FileFormat string
	Size       int64
	Identifier string
	IsReingest bool
}

var changedFiles = []ChangedFile{
	{
		FileFormat: "image/svg+xml",
		Size:       int64(22491),
		Identifier: "test.edu/test.edu.apt-001/data/files/file_example_SVG_20kB.svg",
		IsReingest: false,
	},
	{
		FileFormat: "application/xml",
		Size:       int64(24069),
		Identifier: "test.edu/test.edu.apt-001/data/files/data.xml",
		IsReingest: true,
	},
	{
		FileFormat: "application/json",
		Size:       int64(20556),
		Identifier: "test.edu/test.edu.apt-001/data/files/data.json",
		IsReingest: true,
	},
	{
		FileFormat: "text/csv",
		Size:       int64(284058),
		Identifier: "test.edu/test.edu.apt-001/data/files/data.csv",
		IsReingest: true,
	},
	{
		FileFormat: "application/binary",
		Size:       int64(6148),
		Identifier: "test.edu/test.edu.apt-001/data/files/.DS_Store",
		IsReingest: false,
	},
	{
		FileFormat: "text/plain",
		Size:       int64(452),
		Identifier: "test.edu/test.edu.apt-001/bag-info.txt",
		IsReingest: true,
	},
	{
		FileFormat: "text/plain",
		Size:       int64(125),
		Identifier: "test.edu/test.edu.apt-001/aptrust-info.txt",
		IsReingest: true,
	},
}

func getChangedFileRecord(identifier string) ChangedFile {
	var changedFile ChangedFile
	for _, f := range changedFiles {
		if f.Identifier == identifier {
			changedFile = f
			break
		}
	}
	return changedFile
}

func testObjectUpdate(t *testing.T, context *common.Context) {
	bagPath := getBagPath("updated", "test.edu.apt-001.tar")
	recorder := prepareForRecord(t, bagPath, recorderItemID_02, context)
	require.NotNil(t, recorder)
	fileCount, errors := recorder.Run()
	require.Empty(t, errors)
	assert.Equal(t, 18, fileCount)

	// Since the last step of ingest succeeded, the original
	// tar file in the receiving bucket should be marked for
	// deletion.
	assert.True(t, recorder.IngestObject.ShouldDeleteFromReceiving)

	testUpdatedObjectInRegistry(t, recorder)
}

func testUpdatedObjectInRegistry(t *testing.T, recorder *ingest.Recorder) {
	client := recorder.Context.RegistryClient
	resp := client.IntellectualObjectByIdentifier(recorder.IngestObject.Identifier())
	require.Nil(t, resp.Error)
	intelObj := resp.IntellectualObject()
	require.NotNil(t, intelObj)

	assert.Equal(t, intelObj.Access, constants.AccessInstitution)
	assert.Equal(t, "bag-001-updated", intelObj.AltIdentifier)
	assert.Equal(t, "apt-001-updated", intelObj.BagGroupIdentifier)
	assert.Equal(t, "https://raw.githubusercontent.com/APTrust/preservation-services/master/profiles/aptrust-v2.2.json", intelObj.BagItProfileIdentifier)
	assert.Equal(t, "test.edu.apt-001", intelObj.BagName)
	assert.False(t, intelObj.CreatedAt.IsZero())
	assert.Equal(t, "Updated APTrust bag 001 - updated", intelObj.Description)
	assert.Equal(t, 32, len(intelObj.ETag))
	assert.True(t, intelObj.ID > 0)
	assert.Equal(t, "test.edu/test.edu.apt-001", intelObj.Identifier)
	assert.Equal(t, "test.edu", intelObj.InstitutionIdentifier)
	assert.Equal(t, "bag-001-updated", intelObj.InternalSenderIdentifier)
	assert.Equal(t, "Updated APTrust bag 001 - updated", intelObj.InternalSenderDescription)
	assert.True(t, intelObj.InstitutionID > 0)
	assert.Equal(t, intelObj.SourceOrganization, "Test University")
	assert.Equal(t, intelObj.State, "A")
	assert.Equal(t, intelObj.StorageOption, constants.StorageClassStandard)
	assert.Equal(t, "APTrust Bag 001 (updated)", intelObj.Title)

	assert.True(t, intelObj.UpdatedAt.After(intelObj.CreatedAt))

	testUpdatedObjectEventsInRegistry(t, recorder, intelObj.UpdatedAt)
	testUpdatedFilesInRegistry(t, recorder, intelObj.UpdatedAt)
}

func testUpdatedObjectEventsInRegistry(t *testing.T, recorder *ingest.Recorder, timestamp time.Time) {
	client := recorder.Context.RegistryClient

	params := url.Values{}
	params.Add("intellectual_object_id", strconv.FormatInt(recorder.IngestObject.ID, 10))
	params.Add("generic_file_id__is_null", "true")
	params.Add("per_page", "300")
	params.Add("page", "1")

	resp := client.PremisEventList(params)

	require.Nil(t, resp.Error)
	events := resp.PremisEvents()
	require.NotEmpty(t, events)

	eventTypes := make(map[string]int)
	for _, event := range events {
		if _, ok := eventTypes[event.EventType]; !ok {
			eventTypes[event.EventType] = 0
		}
		eventTypes[event.EventType]++
	}

	// No new creation event, because this is reingest.
	// There should be just one, the original.
	assert.Equal(t, 1, eventTypes[constants.EventCreation])

	// No new identifier assignment for reingest
	// Should be just one, the original assignment event.
	assert.Equal(t, 1, eventTypes[constants.EventIdentifierAssignment])

	// Should be original + one new rights event, since this can be reset
	// on each ingest.
	assert.Equal(t, 2, eventTypes[constants.EventAccessAssignment])

	// There *SHOULD* be a new ingest event for reingest.
	// So, one for original ingest plus one for re-ingest = 2.
	assert.Equal(t, 2, eventTypes[constants.EventIngestion])
}

func testUpdatedFilesInRegistry(t *testing.T, recorder *ingest.Recorder, timestamp time.Time) {
	objIdentifier := recorder.IngestObject.Identifier()
	client := recorder.Context.RegistryClient
	params := url.Values{}
	params.Add("intellectual_object_id", strconv.FormatInt(recorder.IngestObject.ID, 10))
	params.Add("updated_at__gteq", timestamp.Format(time.RFC3339Nano))
	params.Add("per_page", "300")
	params.Add("page", "1")
	resp := client.GenericFileList(params)
	require.Nil(t, resp.Error)
	genericFiles := resp.GenericFiles()

	require.NotEmpty(t, genericFiles)
	assert.Equal(t, len(changedFiles), len(genericFiles))

	for _, changedFile := range changedFiles {
		found := false
		for _, gf := range genericFiles {
			if gf.Identifier == changedFile.Identifier {
				found = true
				break
			}
		}
		assert.True(t, found, changedFile.Identifier)
	}

	for _, gf := range genericFiles {
		assert.False(t, gf.CreatedAt.IsZero())
		assert.True(t, strings.Contains(gf.FileFormat, "/"), "%s - %s", gf.Identifier, gf.FileFormat)
		assert.True(t, gf.ID > 0)
		assert.True(t, strings.HasPrefix(gf.Identifier, objIdentifier))
		assert.True(t, len(gf.Identifier) > len(objIdentifier))
		assert.True(t, gf.InstitutionID > 0)
		assert.True(t, gf.IntellectualObjectID > 0)
		gfObjIdentifier, err := gf.IntellectualObjectIdentifier()
		assert.Nil(t, err)
		assert.Equal(t, objIdentifier, gfObjIdentifier)
		assert.False(t, gf.LastFixityCheck.IsZero())
		assert.True(t, gf.Size > 0)
		assert.Equal(t, "A", gf.State)
		assert.Equal(t, constants.StorageStandard, gf.StorageOption)
		assert.True(t, util.LooksLikeUUID(gf.UUID))
		assert.True(t, gf.UpdatedAt.After(timestamp))

		testUpdatedFileEventsInRegistry(t, recorder, gf, timestamp)
		testUpdatedChecksumsInRegistry(t, recorder, gf)
	}
}

func testUpdatedFileEventsInRegistry(t *testing.T, recorder *ingest.Recorder, gf *registry.GenericFile, timestamp time.Time) {
	objIdentifier, _ := gf.IntellectualObjectIdentifier()
	client := recorder.Context.RegistryClient
	params := url.Values{}
	params.Add("generic_file_id", strconv.FormatInt(gf.ID, 10))
	params.Add("per_page", "100")
	params.Add("page", "1")

	resp := client.PremisEventList(params)
	require.Nil(t, resp.Error)
	events := resp.PremisEvents()
	require.NotEmpty(t, events)

	eventTypes := make(map[string]int)
	for _, event := range events {
		if _, ok := eventTypes[event.EventType]; !ok {
			eventTypes[event.EventType] = 0
		}
		eventTypes[event.EventType]++
		assert.NotEmpty(t, event.Agent)
		assert.NotEmpty(t, event.DateTime)
		assert.NotEmpty(t, event.Detail)
		assert.NotEmpty(t, event.EventType)
		assert.NotEqual(t, 0, event.GenericFileID)
		assert.Equal(t, gf.Identifier, event.GenericFileIdentifier)
		assert.NotEmpty(t, event.Identifier)
		assert.NotEmpty(t, event.InstitutionID)
		assert.NotEmpty(t, event.IntellectualObjectID, event)
		assert.Equal(t, objIdentifier, event.IntellectualObjectIdentifier)
		assert.NotEmpty(t, event.Object)
		assert.NotEmpty(t, event.OutcomeDetail)
		assert.NotEmpty(t, event.OutcomeInformation)
		assert.NotEmpty(t, event.Outcome)
	}

	changedFile := getChangedFileRecord(gf.Identifier)

	// There should be a total of two identifier assignment events:
	// 1) semantic identifier assignment, 2) URL identifier assignment.
	// We do not assign new identifiers on reingest. We keep the same ones,
	// so there should be exactly two identifier assignment events after both
	// initial ingest and reingest.
	if changedFile.IsReingest {
		assert.Equal(t, 2, eventTypes[constants.EventIdentifierAssignment], gf.Identifier)
		// reingest should have two ingest events
		assert.Equal(t, 2, eventTypes[constants.EventIngestion], gf.Identifier)
		// (md5, sha1, sha256, sha512) x 2
		// because the file changed on reingest and has new checksums.
		// The registry keeps current and all historical checksums.
		assert.Equal(t, 8, eventTypes[constants.EventDigestCalculation], gf.Identifier)
		// Replication events from two ingests
		assert.Equal(t, 2, eventTypes[constants.EventReplication], gf.Identifier)
	} else {
		assert.Equal(t, 2, eventTypes[constants.EventIdentifierAssignment], gf.Identifier)
		// if not reingest, there should be just one ingest event
		assert.Equal(t, 1, eventTypes[constants.EventIngestion], gf.Identifier)
		// (md5, sha1, sha256, sha512) x 1
		assert.Equal(t, 4, eventTypes[constants.EventDigestCalculation], gf.Identifier)
		// replication event from one ingest
		assert.Equal(t, 1, eventTypes[constants.EventReplication], gf.Identifier)
	}
}

func testUpdatedChecksumsInRegistry(t *testing.T, recorder *ingest.Recorder, gf *registry.GenericFile) {
	params := url.Values{}
	params.Add("generic_file_id", strconv.FormatInt(gf.ID, 10))
	params.Add("per_page", "100")
	params.Add("page", "1")

	resp := recorder.Context.RegistryClient.ChecksumList(params)
	require.Nil(t, resp.Error)
	checksums := resp.Checksums()

	changedFile := getChangedFileRecord(gf.Identifier)

	// For reingested files, we should have an old and a new
	// md5, sha1, sha256, and sha512 digest. For new files, we
	// should have just one of each.
	if changedFile.IsReingest {
		assert.Equal(t, 8, len(checksums), gf.Identifier)
	} else {
		assert.Equal(t, 4, len(checksums), gf.Identifier)
	}

	for _, gfChecksum := range gf.Checksums {
		found := false
		for _, cs := range checksums {
			if cs.Digest == gfChecksum.Digest {
				found = true
			}
		}
		assert.True(t, found, gfChecksum.Algorithm, "%s (%s)", gf.Identifier, gfChecksum.Algorithm)
	}
}
