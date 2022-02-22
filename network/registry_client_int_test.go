// -- //go:build integration

package network_test

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/network"
	"github.com/APTrust/preservation-services/util/logger"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func GetRegistryClient(t *testing.T) *network.RegistryClient {
	config := common.NewConfig()
	assert.Equal(t, "test", config.ConfigName)
	_logger, _ := logger.InitLogger(config.LogDir, config.LogLevel)
	require.NotNil(t, _logger)
	client, err := network.NewRegistryClient(
		config.RegistryURL,
		config.RegistryAPIVersion,
		config.RegistryAPIUser,
		config.RegistryAPIKey,
		_logger,
	)
	require.Nil(t, err)
	require.NotNil(t, client)
	return client
}

func TestEscapeFileIdentifier(t *testing.T) {
	identifier := "institution2.edu/toads/Prakash_ 39 Harv. J.L. & Pub. Pol’y 341 .pdf"
	expected := "institution2.edu%2Ftoads%2FPrakash_%2039%20Harv.%20J.L.%20%26%20Pub.%20Pol%E2%80%99y%20341%20.pdf"
	assert.Equal(t, expected, network.EscapeFileIdentifier(identifier))
	assert.Equal(t, "test.edu%2Fobj%2Ffile%20name%3F.txt", network.EscapeFileIdentifier("test.edu/obj/file name?.txt"))
}

func TestRegistryInstitutionByIdentifier(t *testing.T) {
	institutions := []string{
		"institution1.edu",
		"institution2.edu",
		"test.edu",
	}
	client := GetRegistryClient(t)
	for _, identifier := range institutions {
		resp := client.InstitutionByIdentifier(identifier)
		assert.NotNil(t, resp)
		require.Nil(t, resp.Error)
		assert.Equal(t,
			fmt.Sprintf("/admin-api/v3/institutions/show/%s", identifier),
			resp.Request.URL.Opaque)
		institution := resp.Institution()
		assert.NotNil(t, institution)
		assert.Equal(t, identifier, institution.Identifier)
	}
}

func TestRegistryInstitutionByID(t *testing.T) {
	client := GetRegistryClient(t)
	for i := 1; i < 5; i++ {
		resp := client.InstitutionByID(int64(i))
		assert.NotNil(t, resp)
		require.Nil(t, resp.Error)
		assert.Equal(t,
			fmt.Sprintf("/admin-api/v3/institutions/show/%d", i),
			resp.Request.URL.Opaque)
		institution := resp.Institution()
		assert.NotNil(t, institution)
		assert.EqualValues(t, i, institution.ID)
	}
}

func TestRegistryInstitutionList(t *testing.T) {
	client := GetRegistryClient(t)
	v := url.Values{}
	v.Add("sort", "name__asc")
	v.Add("per_page", "20")
	resp := client.InstitutionList(v)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	assert.Equal(t,
		fmt.Sprintf("/admin-api/v3/institutions/?%s", v.Encode()),
		resp.Request.URL.Opaque)
	institutions := resp.Institutions()
	assert.Equal(t, 5, len(institutions))
	for _, inst := range institutions {
		assert.NotEmpty(t, inst.ID)
		assert.NotEmpty(t, inst.Name)
		assert.NotEmpty(t, inst.Identifier)
		assert.NotEmpty(t, inst.ReceivingBucket)
		assert.NotEmpty(t, inst.RestoreBucket)
	}
}

func TestRegistryIntellectualObjectGet(t *testing.T) {
	identifier := "institution1.edu/photos"
	expectedURL := fmt.Sprintf("/admin-api/v3/objects/show/%s", network.EscapeFileIdentifier(identifier))
	client := GetRegistryClient(t)
	resp := client.IntellectualObjectByIdentifier(identifier)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.Equal(t, expectedURL, resp.Request.URL.Opaque)
	testRegistryObjectResponse(t, resp)

	obj := resp.IntellectualObject()
	expectedURL = fmt.Sprintf("/admin-api/v3/objects/show/%d", obj.ID)
	resp = client.IntellectualObjectByID(obj.ID)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.Equal(t, expectedURL, resp.Request.URL.Opaque)
	testRegistryObjectResponse(t, resp)
}

func testRegistryObjectResponse(t *testing.T, resp *network.RegistryResponse) {
	obj := resp.IntellectualObject()
	assert.NotNil(t, obj)
	assert.Equal(t, "institution1.edu/photos", obj.Identifier)
	assert.Equal(t, "First Object for Institution One", obj.Title)
	assert.Equal(t, "A bag of photos", obj.Description)
	assert.Equal(t, "photos_from_the_1960s", obj.AltIdentifier)
	assert.Equal(t, "photos.tar", obj.BagName)
	assert.Equal(t, constants.AccessInstitution, obj.Access)
	assert.Equal(t, "institution1.edu", obj.InstitutionIdentifier)
	assert.Equal(t, int64(2), obj.InstitutionID)
	assert.Equal(t, constants.StateActive, obj.State)
	assert.Equal(t, "etagforinst1photos", obj.ETag)
	assert.Equal(t, "Institution One", obj.SourceOrganization)
	assert.Equal(t, "https://example.com/profile.json", obj.BagItProfileIdentifier)
	assert.Equal(t, "First internal identifier", obj.InternalSenderIdentifier)
	assert.Equal(t, "First internal description", obj.InternalSenderDescription)
}

func TestRegistryIntellectualObjectList(t *testing.T) {
	client := GetRegistryClient(t)
	v := url.Values{}
	v.Add("sort", "identifier__asc")
	v.Add("per_page", "20")
	v.Add("storage_option", constants.StorageClassStandard)
	v.Add("state", constants.StateActive)
	resp := client.IntellectualObjectList(v)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	assert.Equal(t, fmt.Sprintf("/admin-api/v3/objects/?%s", v.Encode()), resp.Request.URL.Opaque)
	objects := resp.IntellectualObjects()
	assert.Equal(t, 11, len(objects))
	for _, obj := range objects {
		assert.NotEmpty(t, obj.ID)
		assert.NotEmpty(t, obj.FileCount)
		assert.NotEmpty(t, obj.Identifier)
		assert.NotEmpty(t, obj.Size)
		assert.NotEmpty(t, obj.SourceOrganization)
		assert.Equal(t, constants.StateActive, obj.State)
		assert.Equal(t, constants.StorageClassStandard, obj.StorageOption)
	}
}

func TestRegistryIntellectualObjectSave_Create(t *testing.T) {
	intelObj := testutil.GetIntellectualObject()
	// Id of zero means it's never been saved.
	intelObj.ID = 0

	// Make sure we're using an institution id that was
	// loaded with the test fixtures
	client := GetRegistryClient(t)
	resp := client.InstitutionByIdentifier("test.edu")
	require.Nil(t, resp.Error)
	testInst := resp.Institution()
	require.NotNil(t, testInst)
	intelObj.InstitutionID = testInst.ID

	resp = client.IntellectualObjectSave(intelObj)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	assert.Equal(t, "/admin-api/v3/objects/create/4", resp.Request.URL.Opaque)
	obj := resp.IntellectualObject()
	require.NotNil(t, obj)

	assert.Equal(t, intelObj.Identifier, obj.Identifier)
	assert.NotEqual(t, 0, obj.ID)
	assert.NotEqual(t, intelObj.CreatedAt, obj.CreatedAt)
	assert.NotEqual(t, intelObj.UpdatedAt, obj.UpdatedAt)
	assert.Equal(t, intelObj.Title, obj.Title)
	assert.Equal(t, intelObj.Description, obj.Description)
	assert.Equal(t, intelObj.AltIdentifier, obj.AltIdentifier)
	assert.Equal(t, intelObj.BagName, obj.BagName)
	assert.Equal(t, intelObj.Access, obj.Access)
	assert.EqualValues(t, 4, obj.InstitutionID)
	assert.Equal(t, intelObj.State, obj.State)
	assert.Equal(t, intelObj.ETag, obj.ETag)
	assert.Equal(t, intelObj.SourceOrganization, obj.SourceOrganization)
	assert.Equal(t, intelObj.BagItProfileIdentifier, obj.BagItProfileIdentifier)
	assert.Equal(t, intelObj.InternalSenderIdentifier, obj.InternalSenderIdentifier)
	assert.Equal(t, intelObj.InternalSenderDescription, obj.InternalSenderDescription)
}

func TestRegistryIntellectualObjectSave_Update(t *testing.T) {
	client := GetRegistryClient(t)

	// Get the most recently created object for test.edu
	v := url.Values{}
	v.Add("institution_id", "4") // 4 = test.edu in fixture data
	v.Add("per_page", "1")
	v.Add("sort", "created_at__desc")
	resp := client.IntellectualObjectList(v)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	existingObj := resp.IntellectualObject()
	require.NotNil(t, existingObj)

	newDesc := fmt.Sprintf("** Updated description of test object **")
	existingObj.Description = newDesc
	resp = client.IntellectualObjectSave(existingObj)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	assert.Equal(t,
		fmt.Sprintf("/admin-api/v3/objects/update/%d", existingObj.ID),
		resp.Request.URL.Opaque)
	obj := resp.IntellectualObject()
	assert.NotNil(t, obj)
	assert.Equal(t, existingObj.Identifier, obj.Identifier)
	assert.Equal(t, newDesc, obj.Description)
	assert.NotEqual(t, existingObj.UpdatedAt, obj.UpdatedAt)

	assert.Equal(t, existingObj.Title, obj.Title)
	assert.Equal(t, existingObj.Description, obj.Description)
	assert.Equal(t, existingObj.AltIdentifier, obj.AltIdentifier)
	assert.Equal(t, existingObj.BagName, obj.BagName)
	assert.Equal(t, existingObj.Access, obj.Access)
	assert.Equal(t, existingObj.InstitutionID, obj.InstitutionID)
	assert.Equal(t, existingObj.State, obj.State)
	assert.Equal(t, existingObj.ETag, obj.ETag)
	assert.Equal(t, existingObj.SourceOrganization, obj.SourceOrganization)
	assert.Equal(t, existingObj.BagItProfileIdentifier, obj.BagItProfileIdentifier)
	assert.Equal(t, existingObj.InternalSenderIdentifier, obj.InternalSenderIdentifier)
	assert.Equal(t, existingObj.InternalSenderDescription, obj.InternalSenderDescription)
}

func TestRegistryGenericFileGet(t *testing.T) {
	// From fixture data.
	identifier := "institution1.edu/photos/picture1"
	id := int64(1)

	client := GetRegistryClient(t)
	resp := client.GenericFileByIdentifier(identifier)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.Equal(t, fmt.Sprintf("/admin-api/v3/files/show/%s", network.EscapeFileIdentifier(identifier)), resp.Request.URL.Opaque)
	testRegistryGenericFile(t, resp, identifier, id)

	resp = client.GenericFileByID(id)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.Equal(t, fmt.Sprintf("/admin-api/v3/files/show/%d", id), resp.Request.URL.Opaque)
	testRegistryGenericFile(t, resp, identifier, id)
}

func testRegistryGenericFile(t *testing.T, resp *network.RegistryResponse, identifier string, id int64) {
	gf := resp.GenericFile()
	require.NotNil(t, gf)
	assert.Equal(t, identifier, gf.Identifier)
	assert.Equal(t, id, gf.ID)
	assert.Equal(t, id, gf.IntellectualObjectID) // happens to belong to obj 1
	assert.Equal(t, int64(243855000), gf.Size)
	assert.Equal(t, "image/jpeg", gf.FileFormat)
	assert.Equal(t, constants.StorageStandard, gf.StorageOption)

	assert.Equal(t, 2, len(gf.Checksums))
	assert.Equal(t, 4, len(gf.PremisEvents))
	assert.Equal(t, 2, len(gf.StorageRecords))
}

func TestGenericFileList(t *testing.T) {
	client := GetRegistryClient(t)
	v := url.Values{}
	v.Add("sort", "identifier__asc")
	v.Add("per_page", "100")
	v.Add("institution_id", "2")
	v.Add("storage_option", constants.StorageClassStandard)
	resp := client.GenericFileList(v)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.Equal(t, fmt.Sprintf("/admin-api/v3/files/?%s", v.Encode()), resp.Request.URL.Opaque)
	files := resp.GenericFiles()

	lastIdentifier := ""
	assert.Equal(t, 12, len(files))
	for _, gf := range files {
		assert.EqualValues(t, 2, gf.InstitutionID)
		assert.Equal(t, constants.StorageClassStandard, gf.StorageOption)
		assert.True(t, gf.Identifier > lastIdentifier)
		lastIdentifier = gf.Identifier
	}
}

func TestRegistryGenericFileSave_Create(t *testing.T) {
	client := GetRegistryClient(t)

	v := url.Values{}
	v.Add("sord", "identifier__asc")
	v.Add("per_page", "1")
	resp := client.IntellectualObjectList(v)
	require.Nil(t, resp.Error)
	require.True(t, len(resp.IntellectualObjects()) > 0)
	obj := resp.IntellectualObject()

	gf := testutil.GetGenericFileForObj(obj, 1, false, false)
	require.Equal(t, obj.InstitutionID, gf.InstitutionID)
	resp = client.GenericFileSave(gf)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	assert.Equal(t, fmt.Sprintf("/admin-api/v3/files/create/%d", gf.InstitutionID), resp.Request.URL.Opaque)
	gfSaved := resp.GenericFile()
	require.NotNil(t, gfSaved)
	assert.Equal(t, gf.Identifier, gfSaved.Identifier)
	assert.NotEqual(t, 0, gfSaved.ID)
	assert.NotEqual(t, gf.UpdatedAt, gfSaved.UpdatedAt)

	// Make sure we can save zero-size file.
	// Specific problems with this in testing, as Registry
	// interprets zero as blank or missing value.
	gf = testutil.GetGenericFileForObj(obj, 1, false, false)
	gf.Size = int64(0)
	gf.Identifier = gf.Identifier + "002"
	resp = client.GenericFileSave(gf)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
}

func TestRegistryGenericFileSave_Update(t *testing.T) {
	client := GetRegistryClient(t)
	v := url.Values{}
	v.Add("sort", "identifier__asc")
	v.Add("per_page", "4")
	v.Add("institution_identifier", "aptrust.org")
	resp := client.GenericFileList(v)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	files := resp.GenericFiles()

	for _, gf := range files {
		newSize := gf.Size + 2
		gf.Size = newSize
		resp := client.GenericFileSave(gf)
		assert.NotNil(t, resp)
		assert.Nil(t, resp.Error)
		assert.Equal(t, fmt.Sprintf("/admin-api/v3/files/update/%d", gf.ID), resp.Request.URL.Opaque)
		gfSaved := resp.GenericFile()
		require.NotNil(t, gfSaved)
		assert.Equal(t, gf.Identifier, gfSaved.Identifier)
		assert.Equal(t, newSize, gfSaved.Size)
		assert.NotEqual(t, gf.UpdatedAt, gfSaved.UpdatedAt)
	}
}

func TestRegistryGenericFileSaveBatch(t *testing.T) {
	intelObj := testutil.GetIntellectualObject()

	intelObj.Identifier = "test.edu/TestBag002"
	intelObj.InstitutionID = int64(4) // test.edu id
	client := GetRegistryClient(t)

	resp := client.IntellectualObjectSave(intelObj)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	savedObj := resp.IntellectualObject()
	require.NotNil(t, savedObj)

	// Create 10 new files. Make sure their object ID and institution ID
	// match the parent object.
	files := make([]*registry.GenericFile, 10)
	for i := 0; i < 10; i++ {
		gf := testutil.GetGenericFileForObj(savedObj, i, true, true)
		gf.InstitutionID = intelObj.InstitutionID
		files[i] = gf
	}

	resp = client.GenericFileCreateBatch(files)
	require.Nil(t, resp.Error)

	savedFiles := resp.GenericFiles()
	assert.Equal(t, len(files), len(savedFiles))

	// Make sure Registry actually saved everything
	for i := 0; i < 10; i++ {
		// GenericFiles
		identifier := fmt.Sprintf("%s/object/data/file_%d.txt", savedObj.Identifier, i)
		resp := client.GenericFileByIdentifier(identifier)
		assert.Nil(t, resp.Error)
		assert.NotNil(t, resp.GenericFile(), identifier)

		gf := resp.GenericFile()
		require.NotNil(t, gf)

		// Checksums - can also filter by generic file ID
		v := url.Values{}
		v.Add("generic_file_identifier", gf.Identifier)
		v.Add("per_page", "20")
		resp = client.ChecksumList(v)
		assert.Nil(t, resp.Error)
		checksums := resp.Checksums()
		assert.Equal(t, 2, len(checksums))
		for _, cs := range checksums {
			assert.True(t, cs.ID > 0)
		}

		// PremisEvents - can also filter by generic file ID
		v = url.Values{}
		v.Add("generic_file_identifier", gf.Identifier)
		v.Add("per_page", "20")
		resp = client.PremisEventList(v)
		assert.Nil(t, resp.Error)
		events := resp.PremisEvents()
		assert.Equal(t, 5, len(events))
		for _, event := range events {
			assert.True(t, event.ID > 0)
		}
	}
}

func TestRegistryChecksumByID(t *testing.T) {
	client := GetRegistryClient(t)
	resp := client.ChecksumByID(1)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	cs := resp.Checksum()
	require.NotNil(t, cs)
	assert.EqualValues(t, 1, cs.ID)
	assert.Equal(t, "md5", cs.Algorithm)
	assert.Equal(t, "12345678", cs.Digest)
	assert.EqualValues(t, 1, cs.GenericFileID)
}

func TestRegistryChecksumList(t *testing.T) {
	client := GetRegistryClient(t)
	v := url.Values{}
	v.Add("sort", "generic_file_id__asc")
	v.Add("per_page", "100")
	v.Add("institution_id", "2")
	v.Add("algorithm", constants.AlgSha256)
	resp := client.ChecksumList(v)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.Equal(t, fmt.Sprintf("/admin-api/v3/checksums/?%s", v.Encode()), resp.Request.URL.Opaque)
	checksums := resp.Checksums()
	lastGFID := int64(0)
	assert.Equal(t, 3, len(checksums))
	for _, cs := range checksums {
		assert.EqualValues(t, 2, cs.InstitutionID)
		assert.Equal(t, constants.AlgSha256, cs.Algorithm)
		assert.True(t, cs.GenericFileID > lastGFID)
		lastGFID = cs.GenericFileID
	}
}

func TestRegistryChecksumCreate(t *testing.T) {
	client := GetRegistryClient(t)
	timestamp := time.Now().UTC()
	checksum := &registry.Checksum{
		ID:            0,
		Algorithm:     constants.AlgSha1,
		Digest:        "12345123451234512345",
		DateTime:      timestamp,
		GenericFileID: 11,
		InstitutionID: 3, //belongs to institution2.edu
	}
	resp := client.ChecksumCreate(checksum)
	require.Nil(t, resp.Error)
	assert.Equal(t, http.StatusCreated, resp.Response.StatusCode)
	savedChecksum := resp.Checksum()
	require.NotNil(t, savedChecksum)

	assert.True(t, savedChecksum.ID > 0)
	assert.Equal(t, checksum.Algorithm, savedChecksum.Algorithm)
	assert.Equal(t, checksum.Digest, savedChecksum.Digest)
	assert.Equal(t, checksum.GenericFileID, savedChecksum.GenericFileID)
	assert.Equal(t, timestamp, savedChecksum.DateTime)

	// Make sure this now shows as the generic file's official Sha1
	resp = client.GenericFileByID(11)
	require.Nil(t, resp.Error)
	gf := resp.GenericFile()
	require.NotNil(t, gf)

	foundChecksum := false
	for _, cs := range gf.Checksums {
		if cs.Digest == checksum.Digest {
			foundChecksum = true
			break
		}
	}
	assert.True(t, foundChecksum)
}

func TestRegistryPremisEventGet(t *testing.T) {
	// These values come from registry fixtures in premis_events.csv
	id := int64(1)
	identifier := "a966ca54-ee5b-4606-81bd-7653dd5f3a63"

	client := GetRegistryClient(t)
	resp := client.PremisEventByIdentifier(identifier)
	require.Nil(t, resp.Error)
	event := resp.PremisEvent()
	require.NotNil(t, event)
	testPremisEvent(t, event, id, identifier)

	resp = client.PremisEventByID(id)
	require.Nil(t, resp.Error)
	event = resp.PremisEvent()
	require.NotNil(t, event)
	testPremisEvent(t, event, id, identifier)
}

func testPremisEvent(t *testing.T, event *registry.PremisEvent, id int64, identifier string) {
	require.NotNil(t, event)
	assert.Equal(t, id, event.ID)
	assert.Equal(t, identifier, event.Identifier)
	assert.Equal(t, constants.EventIngestion, event.EventType)
	assert.NotEmpty(t, event.DateTime)
}

func TestRegistryPremisEventList(t *testing.T) {
	client := GetRegistryClient(t)
	v := url.Values{}
	v.Add("sort", "identifier__asc")
	v.Add("per_page", "100")
	v.Add("institution_id", "2")
	resp := client.PremisEventList(v)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.Equal(t, fmt.Sprintf("/admin-api/v3/events/?%s", v.Encode()), resp.Request.URL.Opaque)
	events := resp.PremisEvents()

	lastIdentifier := ""
	assert.Equal(t, 30, len(events))
	for _, event := range events {
		assert.EqualValues(t, 2, event.InstitutionID)
		assert.True(t, event.Identifier > lastIdentifier)
		lastIdentifier = event.Identifier
	}
}

func TestRegistryStorageRecordCreate(t *testing.T) {
	client := GetRegistryClient(t)
	sr := &registry.StorageRecord{
		ID:            0,
		GenericFileID: 11,
		URL:           "https://example.com/storage/blah-blah-test",
	}
	institutionID := int64(3) // owner of file 11 in fixtures
	resp := client.StorageRecordCreate(sr, institutionID)
	require.Nil(t, resp.Error)
	assert.Equal(t, http.StatusCreated, resp.Response.StatusCode)
	savedStorageRecord := resp.StorageRecord()
	require.NotNil(t, savedStorageRecord)

	assert.True(t, savedStorageRecord.ID > 0)
	assert.Equal(t, sr.URL, savedStorageRecord.URL)
	assert.Equal(t, sr.GenericFileID, savedStorageRecord.GenericFileID)
}

func TestRegistryStorageRecordList(t *testing.T) {
	client := GetRegistryClient(t)
	v := url.Values{}
	v.Add("sort", "url__asc")
	v.Add("per_page", "100")
	v.Add("generic_file_id", "1")
	resp := client.StorageRecordList(v)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.Equal(t, "/admin-api/v3/storage_records/?generic_file_id=1&per_page=100&sort=url__asc", resp.Request.URL.Opaque)
	records := resp.StorageRecords()

	lastUrl := ""
	assert.Equal(t, 2, len(records))
	for _, sr := range records {
		assert.EqualValues(t, 1, sr.GenericFileID)
		assert.True(t, sr.URL > lastUrl)
		lastUrl = sr.URL
	}
}

func TestWorkItemByID(t *testing.T) {
	client := GetRegistryClient(t)
	resp := client.WorkItemByID(1)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	item := resp.WorkItem()
	require.NotNil(t, item)
	assert.EqualValues(t, 1, item.ID)
	assert.Equal(t, constants.ActionIngest, item.Action)
	assert.EqualValues(t, 2, item.InstitutionID)
	assert.EqualValues(t, "fake_bag_01.tar", item.Name)
	assert.NotEmpty(t, item.BagDate)
	assert.Equal(t, "", item.ObjectIdentifier)
}

func TestWorkItemList(t *testing.T) {
	client := GetRegistryClient(t)
	v := url.Values{}
	v.Add("sort", "name__desc")
	v.Add("per_page", "100")
	v.Add("institution_id", "2")
	v.Add("action", constants.ActionIngest)
	resp := client.WorkItemList(v)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	assert.Equal(t, fmt.Sprintf("/admin-api/v3/items/?%s", v.Encode()), resp.Request.URL.Opaque)
	items := resp.WorkItems()
	lastName := "zzzzzz"
	assert.Equal(t, 14, len(items))
	for _, item := range items {
		assert.EqualValues(t, 2, item.InstitutionID)
		assert.Equal(t, constants.ActionIngest, item.Action)
		assert.True(t, item.Name < lastName, item.Name)
		lastName = item.Name
	}
}

func TestWorkItemSave(t *testing.T) {
	client := GetRegistryClient(t)

	item := &registry.WorkItem{
		Action:        constants.ActionIngest,
		BagDate:       time.Now().Add(-24 * time.Hour),
		Bucket:        "aptrust.receiving.test.edu",
		DateProcessed: time.Now().UTC(),
		ETag:          "oldmacdonaldhadafarmeieioandonhisfarm",
		InstitutionID: 4,
		Name:          "spongebob.tar",
		Note:          "Item is in receiving bucket",
		Outcome:       "Item is awaiting ingest",
		Retry:         true,
		Size:          38012337,
		Stage:         constants.StageReceive,
		Status:        constants.StatusPending,
		User:          "system@aptrust.org",
	}

	resp := client.WorkItemSave(item)
	require.Nil(t, resp.Error)
	assert.Equal(t, http.StatusCreated, resp.Response.StatusCode)
	savedWorkItem := resp.WorkItem()
	require.NotNil(t, savedWorkItem)
	assert.True(t, savedWorkItem.ID > 0)
	assert.Equal(t, item.Action, savedWorkItem.Action)
	assert.Equal(t, item.ETag, savedWorkItem.ETag)
	assert.Equal(t, item.Name, savedWorkItem.Name)
	assert.Equal(t, item.InstitutionID, savedWorkItem.InstitutionID)

	// Make sure we can update the item
	savedWorkItem.Status = constants.StatusCancelled
	savedWorkItem.Outcome = "Krabby Patties"
	savedWorkItem.Note = "Patrick Star"
	resp = client.WorkItemSave(savedWorkItem)
	require.Nil(t, resp.Error)
	assert.Equal(t, http.StatusOK, resp.Response.StatusCode)
	updatedWorkItem := resp.WorkItem()
	require.NotNil(t, updatedWorkItem)
	assert.True(t, updatedWorkItem.ID > 0)
	assert.Equal(t, savedWorkItem.Status, updatedWorkItem.Status)
	assert.Equal(t, savedWorkItem.Outcome, updatedWorkItem.Outcome)
	assert.Equal(t, savedWorkItem.Note, updatedWorkItem.Note)
}
