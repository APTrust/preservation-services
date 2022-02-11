//go:build integration
// +build integration

package network_test

import (
	//	"bytes"
	//	"encoding/json"
	"fmt"
	"net/url"
	//	"strings"
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/network"
	"github.com/APTrust/preservation-services/util/logger"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Registry rules say we can't restore an item that's being deleted
// or delete an item that's being restored. To avoid errors in our
// integration tests, make sure we test different object for restore
// and delete. These ids come from the integration test fixtures.
// const ObjIdToDelete = "institution2.edu/coal"
// const ObjIdToRestore = "institution2.edu/toads"
// const FileIdToRestore = "institution2.edu/coal/doc3"
// const FileIdWithChecksums = "institution1.edu/photos/picture1"

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
	assert.Equal(t, 8, len(objects))
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
	assert.Equal(t, existingObj.Institution, obj.Institution)
	assert.Equal(t, existingObj.State, obj.State)
	assert.Equal(t, existingObj.ETag, obj.ETag)
	assert.Equal(t, existingObj.SourceOrganization, obj.SourceOrganization)
	assert.Equal(t, existingObj.BagItProfileIdentifier, obj.BagItProfileIdentifier)
	assert.Equal(t, existingObj.InternalSenderIdentifier, obj.InternalSenderIdentifier)
	assert.Equal(t, existingObj.InternalSenderDescription, obj.InternalSenderDescription)
}

func TestRegistryIntellectualObjectDelete(t *testing.T) {
	// TODO: This requires considerable setup.
	// See the comments on RegistryClient.IntellectualObjectDelete.
	// Come back to it later, when we're further into integration tests.
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
	assert.Equal(t, 11, len(files))
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

func TestRegistryGenericFileDelete(t *testing.T) {
	// TODO: This requires considerable setup.
	// See the comments on RegistryClient.GenericFileDelete.
	// Come back to it later, when we're further into integration tests.
}
