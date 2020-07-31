// +build integration

package network_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
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

// Pharos rules say we can't restore an item that's being deleted
// or delete an item that's being restored. To avoid errors in our
// integration tests, make sure we test different object for restore
// and delete. These ids come from the integration test fixtures.
const ObjIdToDelete = "institution2.edu/coal"
const ObjIdToRestore = "institution2.edu/toads"
const FileIdToRestore = "institution2.edu/coal/doc3"
const FileIdWithChecksums = "institution1.edu/photos/picture1"

var InstFixtures map[string]*registry.Institution
var FileFixtures map[string]*registry.GenericFile
var ObjectFixtures map[string]*registry.IntellectualObject
var EventFixtures map[string]*registry.PremisEvent
var WorkItemFixtures map[string]*registry.WorkItem

func LoadPharosFixtures(t *testing.T) {
	if len(InstFixtures) == 0 {
		InstFixtures = LoadInstitutionFixtures(t)
		FileFixtures = LoadGenericFileFixtures(t)
		ObjectFixtures = LoadObjectFixtures(t)
		EventFixtures = LoadEventFixtures(t)
		WorkItemFixtures = LoadWorkItemFixtures(t)
	}
}

func LoadInstitutionFixtures(t *testing.T) map[string]*registry.Institution {
	data, err := testutil.ReadPharosFixture("institutions.json")
	require.Nil(t, err)
	institutions := make(map[string]*registry.Institution)
	err = json.Unmarshal(data, &institutions)
	require.Nil(t, err)
	return institutions
}

func LoadGenericFileFixtures(t *testing.T) map[string]*registry.GenericFile {
	data, err := testutil.ReadPharosFixture("generic_files.json")
	require.Nil(t, err)
	files := make(map[string]*registry.GenericFile)
	err = json.Unmarshal(data, &files)
	require.Nil(t, err)
	return files
}

func LoadObjectFixtures(t *testing.T) map[string]*registry.IntellectualObject {
	data, err := testutil.ReadPharosFixture("intellectual_objects.json")
	require.Nil(t, err)
	objects := make(map[string]*registry.IntellectualObject)
	err = json.Unmarshal(data, &objects)
	require.Nil(t, err)
	return objects
}

func LoadEventFixtures(t *testing.T) map[string]*registry.PremisEvent {
	data, err := testutil.ReadPharosFixture("premis_events.json")
	require.Nil(t, err)
	events := make(map[string]*registry.PremisEvent)
	err = json.Unmarshal(data, &events)
	require.Nil(t, err)
	return events
}

func LoadWorkItemFixtures(t *testing.T) map[string]*registry.WorkItem {
	data, err := testutil.ReadPharosFixture("work_items.json")
	require.Nil(t, err)
	items := make(map[string]*registry.WorkItem)
	err = json.Unmarshal(data, &items)
	require.Nil(t, err)
	return items
}

func GetPharosClient(t *testing.T) *network.PharosClient {
	config := common.NewConfig()
	assert.Equal(t, "test", config.ConfigName)
	_logger, _ := logger.InitLogger(config.LogDir, config.LogLevel)
	require.NotNil(t, _logger)
	client, err := network.NewPharosClient(
		config.PharosURL,
		config.PharosAPIVersion,
		config.PharosAPIUser,
		config.PharosAPIKey,
		_logger,
	)
	require.Nil(t, err)
	require.NotNil(t, client)
	return client
}

func GetInstitution(t *testing.T, identifier string) *registry.Institution {
	client := GetPharosClient(t)
	resp := client.InstitutionGet(identifier)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	institution := resp.Institution()
	require.NotNil(t, institution)
	return institution
}

func GetObject(t *testing.T, identifier string) *registry.IntellectualObject {
	client := GetPharosClient(t)
	resp := client.IntellectualObjectGet(identifier)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	obj := resp.IntellectualObject()
	require.NotNil(t, obj)
	return obj
}

func GetWorkItem(t *testing.T, etag string) *registry.WorkItem {
	client := GetPharosClient(t)
	v := url.Values{}
	v.Add("etag", etag)
	resp := client.WorkItemList(v)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	item := resp.WorkItem()
	require.NotNil(t, item)
	return item
}

// Returns the only two checksums in the fixture data
func GetChecksums(t *testing.T) []*registry.Checksum {
	client := GetPharosClient(t)
	v := url.Values{}
	v.Add("generic_file_identifier", FileIdWithChecksums)
	resp := client.ChecksumList(v)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	checksums := resp.Checksums()
	require.Equal(t, 2, len(checksums))
	return checksums
}

func TestEscapeFileIdentifier(t *testing.T) {
	identifier := "institution2.edu/toads/Prakash_ 39 Harv. J.L. & Pub. Pol’y 341 .pdf"
	expected := "institution2.edu%2Ftoads%2FPrakash_%2039%20Harv.%20J.L.%20%26%20Pub.%20Pol%E2%80%99y%20341%20.pdf"
	assert.Equal(t, expected, network.EscapeFileIdentifier(identifier))

	assert.Equal(t,
		"test.edu%2Fobj%2Ffile%20name%3F.txt",
		network.EscapeFileIdentifier("test.edu/obj/file name?.txt"))
}

func TestPharosInstitutionGet(t *testing.T) {
	LoadPharosFixtures(t)
	client := GetPharosClient(t)
	for _, inst := range InstFixtures {
		resp := client.InstitutionGet(inst.Identifier)
		assert.NotNil(t, resp)
		require.Nil(t, resp.Error)
		assert.Equal(t,
			fmt.Sprintf("/api/v2/institutions/%s/", inst.Identifier),
			resp.Request.URL.Opaque)
		institution := resp.Institution()
		assert.NotNil(t, institution)
		assert.Equal(t, inst.Identifier, institution.Identifier)
	}
}

func TestPharosInstitutionList(t *testing.T) {
	LoadPharosFixtures(t)
	client := GetPharosClient(t)
	v := url.Values{}
	v.Add("order", "name")
	v.Add("per_page", "20")
	resp := client.InstitutionList(v)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	assert.Equal(t,
		fmt.Sprintf("/api/v2/institutions/?%s", v.Encode()),
		resp.Request.URL.Opaque)
	institutions := resp.Institutions()
	// We have one more institution than what's in the fixtures
	// because a rake task creates staging.edu when the Pharos
	// container starts up.
	assert.Equal(t, len(InstFixtures), len(institutions))

	// Make sure we got the expected items in our list of 4
	for _, inst := range institutions {
		if inst.Identifier != "staging.edu" {
			assert.NotNil(t, InstFixtures[inst.Identifier])
		}
	}
}

func TestPharosIntellectualObjectGet(t *testing.T) {
	LoadPharosFixtures(t)
	client := GetPharosClient(t)
	for _, obj := range ObjectFixtures {
		resp := client.IntellectualObjectGet(obj.Identifier)
		assert.NotNil(t, resp)
		require.Nil(t, resp.Error)
		assert.Equal(t,
			fmt.Sprintf("/api/v2/objects/%s", network.EscapeFileIdentifier(obj.Identifier)),
			resp.Request.URL.Opaque)
		intelObj := resp.IntellectualObject()
		assert.NotNil(t, intelObj)
		assert.Equal(t, obj.Identifier, intelObj.Identifier)
		assert.Equal(t, obj.Title, intelObj.Title)
		assert.Equal(t, obj.Description, intelObj.Description)
		assert.Equal(t, obj.AltIdentifier, intelObj.AltIdentifier)
		assert.Equal(t, obj.BagName, intelObj.BagName)
		assert.Equal(t, obj.Access, intelObj.Access)
		assert.Equal(t, obj.Institution, intelObj.Institution)
		assert.Equal(t, obj.State, intelObj.State)
		assert.Equal(t, obj.ETag, intelObj.ETag)
		assert.Equal(t, obj.SourceOrganization, intelObj.SourceOrganization)
		assert.Equal(t, obj.BagItProfileIdentifier, intelObj.BagItProfileIdentifier)
		assert.Equal(t, obj.InternalSenderIdentifier, intelObj.InternalSenderIdentifier)
		assert.Equal(t, obj.InternalSenderDescription, intelObj.InternalSenderDescription)
	}
}

func TestPharosIntellectualObjectList(t *testing.T) {
	LoadPharosFixtures(t)
	client := GetPharosClient(t)
	v := url.Values{}
	v.Add("order", "identifier")
	v.Add("per_page", "20")
	resp := client.IntellectualObjectList(v)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	assert.Equal(t,
		fmt.Sprintf("/api/v2/objects/?%s", v.Encode()),
		resp.Request.URL.Opaque)
	objects := resp.IntellectualObjects()

	// Should contain all fixtures. May also contain
	// objects created by other integration tests.
	assert.True(t, len(objects) >= len(ObjectFixtures))

	// Make sure all our known fixtures are there
	for identifier, _ := range ObjectFixtures {
		foundInResults := false
		for _, obj := range objects {
			if obj.Identifier == identifier {
				foundInResults = true
				break
			}
		}
		assert.True(t, foundInResults, identifier)
	}
}

func TestPharosIntellectualObjectSave_Create(t *testing.T) {
	intelObj := testutil.GetIntellectualObject()

	// Make sure we're using an institution id that was
	// loaded with the test fixtures
	testInst := GetInstitution(t, "test.edu")
	intelObj.InstitutionID = testInst.ID

	// Id of zero means it's never been saved.
	require.Equal(t, 0, intelObj.ID)

	client := GetPharosClient(t)
	resp := client.IntellectualObjectSave(intelObj)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	assert.Equal(t,
		"/api/v2/objects/test.edu",
		resp.Request.URL.Opaque)
	obj := resp.IntellectualObject()
	require.NotNil(t, obj)
	assert.Equal(t, intelObj.Identifier, obj.Identifier)
	assert.NotEqual(t, 0, obj.ID)
	assert.NotEqual(t, intelObj.UpdatedAt, obj.UpdatedAt)
	assert.Equal(t, intelObj.Title, obj.Title)
	assert.Equal(t, intelObj.Description, obj.Description)
	assert.Equal(t, intelObj.AltIdentifier, obj.AltIdentifier)
	assert.Equal(t, intelObj.BagName, obj.BagName)
	assert.Equal(t, intelObj.Access, obj.Access)
	assert.Equal(t, intelObj.Institution, obj.Institution)
	assert.Equal(t, intelObj.State, obj.State)
	assert.Equal(t, intelObj.ETag, obj.ETag)
	assert.Equal(t, intelObj.SourceOrganization, obj.SourceOrganization)
	assert.Equal(t, intelObj.BagItProfileIdentifier, obj.BagItProfileIdentifier)
	assert.Equal(t, intelObj.InternalSenderIdentifier, obj.InternalSenderIdentifier)
	assert.Equal(t, intelObj.InternalSenderDescription, obj.InternalSenderDescription)
}

func TestPharosIntellectualObjectSave_Update(t *testing.T) {
	LoadPharosFixtures(t)
	client := GetPharosClient(t)

	v := url.Values{}
	v.Add("order", "identifier")
	v.Add("per_page", "20")
	resp := client.IntellectualObjectList(v)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	objects := resp.IntellectualObjects()

	for i, existingObj := range objects {
		newDesc := fmt.Sprintf("New description %d", i)
		existingObj.Description = newDesc
		resp := client.IntellectualObjectSave(existingObj)
		assert.NotNil(t, resp)
		assert.Nil(t, resp.Error)
		assert.Equal(t,
			fmt.Sprintf("/api/v2/objects/%s", network.EscapeFileIdentifier(existingObj.Identifier)),
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
}

func TestIntellectualObjectRequestRestore(t *testing.T) {
	LoadPharosFixtures(t)
	client := GetPharosClient(t)
	resp := client.IntellectualObjectRequestRestore(ObjIdToRestore)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	workItem := resp.WorkItem()
	assert.NotNil(t, workItem)
	assert.Equal(t, ObjIdToRestore, workItem.ObjectIdentifier)
	assert.Equal(t, constants.ActionRestore, workItem.Action)
}

func TestIntellectualObjectRequestDelete(t *testing.T) {
	LoadPharosFixtures(t)
	client := GetPharosClient(t)
	// This call returns no data, so just ensure URL is correct
	// and there's no error.
	resp := client.IntellectualObjectRequestDelete(ObjIdToDelete)
	assert.NotNil(t, resp)

	assert.Equal(t,
		fmt.Sprintf("/api/v2/objects/%s/delete", network.EscapeFileIdentifier(ObjIdToDelete)),
		resp.Request.URL.Opaque)
	require.Nil(t, resp.Error)
}

// ---------------------------------------------------------------------
// This one is untestable in the integration test environment,
// because it requires confirmation tokens and approvals.
// We'll come back to this test later. Note that the we don't
// currently expose the deletion API anyway, so no one is actually
// hitting this endpoint in demo or production.
// ---------------------------------------------------------------------

// func TestIntellectualObjectFinishDelete(t *testing.T) {
// 	LoadPharosFixtures(t)
// 	client := GetPharosClient(t)
// 	// This call returns no data, so just ensure URL is correct
// 	// and there's no error.
// 	resp := client.IntellectualObjectFinishDelete(ObjIdToDelete)
// 	assert.NotNil(t, resp)

// 	assert.Equal(t,
// 		fmt.Sprintf("/api/v2/objects/%s/finish_delete", network.EscapeFileIdentifier(ObjIdToDelete)),
// 		resp.Request.URL.Opaque)
// 	require.Nil(t, resp.Error)
// }

func TestGenericFileGet(t *testing.T) {
	LoadPharosFixtures(t)
	client := GetPharosClient(t)
	for _, gf := range FileFixtures {
		resp := client.GenericFileGet(gf.Identifier)
		assert.NotNil(t, resp)
		require.Nil(t, resp.Error)
		assert.Equal(t,
			fmt.Sprintf("/api/v2/files/%s", network.EscapeFileIdentifier(gf.Identifier)),
			resp.Request.URL.Opaque)
		genericFile := resp.GenericFile()
		assert.NotNil(t, genericFile)

		assert.Equal(t, gf.Identifier, genericFile.Identifier)
		assert.Equal(t, gf.Size, genericFile.Size)
		assert.Equal(t, gf.FileFormat, genericFile.FileFormat)
		assert.Equal(t,
			gf.IntellectualObjectIdentifier,
			genericFile.IntellectualObjectIdentifier)
		assert.Equal(t, constants.StorageStandard, genericFile.StorageOption)
	}
}

func TestGenericFileList(t *testing.T) {
	LoadPharosFixtures(t)
	client := GetPharosClient(t)

	// Note that setting institution_identifier to aptrust.org
	// returns all file for all institutions. If integration
	// tests continue to grow, we may have to increase per_page
	// below to get all files.
	v := url.Values{}
	v.Add("order", "identifier")
	v.Add("per_page", "100")
	v.Add("institution_identifier", "aptrust.org")
	resp := client.GenericFileList(v)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	assert.Equal(t,
		fmt.Sprintf("/api/v2/files/?%s", v.Encode()),
		resp.Request.URL.Opaque)
	files := resp.GenericFiles()

	// Pharos may have a few more files than our fixtures know
	// about, but it should return one record for each of
	// our fixtures.
	assert.True(t, len(files) > len(FileFixtures))
	for _, gf := range FileFixtures {
		gotFileFromPharos := false
		for _, retrievedFile := range files {
			if retrievedFile.Identifier == gf.Identifier {
				gotFileFromPharos = true
				break
			}
		}
		assert.True(t, gotFileFromPharos, gf.Identifier)
	}
}

func TestPharosGenericFileSave_Create(t *testing.T) {
	client := GetPharosClient(t)

	v := url.Values{}
	v.Add("order", "identifier")
	v.Add("per_page", "1")
	resp := client.IntellectualObjectList(v)
	require.Nil(t, resp.Error)
	require.True(t, len(resp.IntellectualObjects()) > 0)
	obj := resp.IntellectualObject()

	gf := testutil.GetGenericFileForObj(obj, 1, false, false)
	resp = client.GenericFileSave(gf)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	assert.Equal(t,
		"/api/v2/files/institution2.edu%2Ftoads",
		resp.Request.URL.Opaque)
	gfSaved := resp.GenericFile()
	require.NotNil(t, gfSaved)
	assert.Equal(t, gf.Identifier, gfSaved.Identifier)
	assert.NotEqual(t, 0, gfSaved.ID)
	assert.NotEqual(t, gf.UpdatedAt, gfSaved.UpdatedAt)

	// Make sure we can save zero-size file.
	// Specific problems with this in testing, as Pharos
	// interprets zero as blank or missing value.
	gf = testutil.GetGenericFileForObj(obj, 1, false, false)
	gf.Size = int64(0)
	gf.Identifier = gf.Identifier + "002"
	resp = client.GenericFileSave(gf)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
}

func TestPharosGenericFileSave_Update(t *testing.T) {
	LoadPharosFixtures(t)
	client := GetPharosClient(t)

	v := url.Values{}
	v.Add("order", "identifier")
	v.Add("per_page", "20")
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
		assert.Equal(t,
			fmt.Sprintf("/api/v2/files/%s", network.EscapeFileIdentifier(gf.Identifier)),
			resp.Request.URL.Opaque)
		gfSaved := resp.GenericFile()
		require.NotNil(t, gfSaved)
		assert.Equal(t, gf.Identifier, gfSaved.Identifier)
		assert.Equal(t, newSize, gfSaved.Size)
		assert.NotEqual(t, gf.UpdatedAt, gfSaved.UpdatedAt)
	}
}

func TestPharosGenericFileSaveBatch(t *testing.T) {
	LoadPharosFixtures(t)
	intelObj := testutil.GetIntellectualObject()

	testInst := GetInstitution(t, "test.edu")
	intelObj.Identifier = "test.edu/TestBag002"
	intelObj.InstitutionID = testInst.ID
	client := GetPharosClient(t)

	resp := client.IntellectualObjectSave(intelObj)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	savedObj := resp.IntellectualObject()
	require.NotNil(t, savedObj)

	files := make([]*registry.GenericFile, 10)
	for i := 0; i < 10; i++ {
		files[i] = testutil.GetGenericFileForObj(savedObj, i, true, true)
	}

	resp = client.GenericFileSaveBatch(files)
	require.Nil(t, resp.Error)

	savedFiles := resp.GenericFiles()
	assert.Equal(t, len(files), len(savedFiles))

	// Make sure Pharos actually saved everything
	for i := 0; i < 10; i++ {
		// GenericFiles
		identifier := fmt.Sprintf("%s/object/data/file_%d.txt", savedObj.Identifier, i)
		resp := client.GenericFileGet(identifier)
		assert.Nil(t, resp.Error)
		assert.NotNil(t, resp.GenericFile(), identifier)

		// Checksums
		v := url.Values{}
		v.Add("generic_file_identifier", identifier)
		v.Add("per_page", "20")
		resp = client.ChecksumList(v)
		assert.Nil(t, resp.Error)
		assert.Equal(t, 2, len(resp.Checksums()))

		// PremisEvents
		v = url.Values{}
		v.Add("file_identifier", identifier)
		v.Add("per_page", "20")
		resp = client.PremisEventList(v)
		assert.Nil(t, resp.Error)
		assert.Equal(t, 5, len(resp.PremisEvents()))
	}
}

func TestPharosGenericFileRequestRestore(t *testing.T) {
	LoadPharosFixtures(t)
	client := GetPharosClient(t)
	resp := client.GenericFileRequestRestore(FileIdToRestore)
	require.Nil(t, resp.Error)
	workItem := resp.WorkItem()
	require.NotNil(t, workItem)
	assert.Equal(t, FileIdToRestore, workItem.GenericFileIdentifier)
	assert.Equal(t, constants.ActionRestore, workItem.Action)
}

// Come back to this one. Requires creation of deletion PREMIS
// event first.
//
// func TestPharosGenericFileFinishDelete(t *testing.T) {
// 	LoadPharosFixtures(t)
// 	client := GetPharosClient(t)
// 	resp := client.GenericFileFinishDelete(FileIdToRestore)
// 	require.Nil(t, resp.Error)
// }

// It's easier to group these tests because we don't have any checksum
// fixtures.
func TestPharosChecksumSaveAndList(t *testing.T) {
	LoadPharosFixtures(t)
	client := GetPharosClient(t)
	resp := client.GenericFileGet(FileIdToRestore)
	require.Nil(t, resp.Error)
	gf := resp.GenericFile()
	require.NotNil(t, gf)
	md5Checksum := testutil.GetChecksum(gf, constants.AlgMd5)

	// Save it
	resp = client.ChecksumSave(md5Checksum, gf.Identifier)
	require.Nil(t, resp.Error)
	savedChecksum := resp.Checksum()
	require.NotNil(t, savedChecksum)
	require.NotEqual(t, 0, savedChecksum.ID)

	// Make sure we can get it back.
	resp = client.ChecksumGet(savedChecksum.ID)
	require.Nil(t, resp.Error)
	retrievedChecksum := resp.Checksum()
	require.NotNil(t, retrievedChecksum)
	require.Equal(t, savedChecksum.ID, retrievedChecksum.ID)

	// Add one more
	sha256Checksum := testutil.GetChecksum(gf, constants.AlgSha256)
	resp = client.ChecksumSave(sha256Checksum, gf.Identifier)
	require.Nil(t, resp.Error)

	// Make sure list returns the right checksums
	v := url.Values{}
	v.Add("generic_file_identifier", gf.Identifier)
	v.Add("per_page", "20")
	resp = client.ChecksumList(v)
	require.Nil(t, resp.Error)
	checksums := resp.Checksums()
	assert.Equal(t, 2, len(checksums))
	for _, cs := range checksums {
		assert.Equal(t, gf.ID, cs.GenericFileID)
		assert.NotEqual(t, 0, cs.ID)
	}

	v.Add("algorithm", constants.AlgMd5)
	resp = client.ChecksumList(v)
	require.Nil(t, resp.Error)
	checksums = resp.Checksums()
	assert.Equal(t, 1, len(checksums))
	for _, cs := range checksums {
		assert.Equal(t, gf.ID, cs.GenericFileID)
		assert.Equal(t, constants.AlgMd5, cs.Algorithm)
	}

	v.Del("algorithm")
	v.Add("algorithm", constants.AlgSha256)
	resp = client.ChecksumList(v)
	require.Nil(t, resp.Error)
	checksums = resp.Checksums()
	assert.Equal(t, 1, len(checksums))
	for _, cs := range checksums {
		assert.Equal(t, gf.ID, cs.GenericFileID)
		assert.Equal(t, constants.AlgSha256, cs.Algorithm)
	}
}

func TestPharosPremisEventGet(t *testing.T) {
	// From testdata/pharos/fixtures.
	fixtureEventId := "be86ea36-4642-4cf7-a29a-80a860ebea82"
	LoadPharosFixtures(t)
	client := GetPharosClient(t)
	resp := client.PremisEventGet(fixtureEventId)
	require.Nil(t, resp.Error)
	event := resp.PremisEvent()
	require.NotNil(t, event)
}

func TestPharosPremisEventsList(t *testing.T) {
	// From testdata/pharos/fixtures.
	fixtureObjIdentifier := "institution1.edu/pdfs"
	fixtureFileIdentifier := "institution1.edu/pdfs/doc2"

	LoadPharosFixtures(t)
	client := GetPharosClient(t)

	v := url.Values{}
	v.Add("file_identifier", fixtureFileIdentifier)
	v.Add("per_page", "20")

	// By file identifier
	resp := client.PremisEventList(v)
	require.Nil(t, resp.Error)
	events := resp.PremisEvents()
	require.Equal(t, 2, len(events))

	// By file identifier & event type
	v.Add("event_type", constants.EventDigestCalculation)
	resp = client.PremisEventList(v)
	require.Nil(t, resp.Error)
	events = resp.PremisEvents()
	require.Equal(t, 1, len(events))

	// By object identifier
	v = url.Values{}
	v.Add("object_identifier", fixtureObjIdentifier)
	v.Add("per_page", "20")
	resp = client.PremisEventList(v)
	require.Nil(t, resp.Error)
	events = resp.PremisEvents()
	require.Equal(t, 7, len(events))

	// By object identifier & event type
	v.Add("event_type", constants.EventIngestion)
	resp = client.PremisEventList(v)
	require.Nil(t, resp.Error)
	events = resp.PremisEvents()
	require.Equal(t, 4, len(events))
}

func TestPharosPremisEventSave(t *testing.T) {
	LoadPharosFixtures(t)
	// obj & file identifiers come from fixture data
	inst := GetInstitution(t, "institution1.edu")
	obj := GetObject(t, "institution1.edu/glass")
	event := &registry.PremisEvent{
		Identifier:                   "91ad5b2c-64bf-4561-8ce7-e62614843786",
		EventType:                    constants.EventIngestion,
		DateTime:                     testutil.Bloomsday,
		OutcomeDetail:                "Object ingested successfully",
		Detail:                       "We got the object into the repository",
		OutcomeInformation:           "Object was ingested",
		Object:                       "Exchange ingest code",
		Agent:                        "https://github.com/APTrust/exchange",
		IntellectualObjectID:         obj.ID,
		IntellectualObjectIdentifier: obj.Identifier,
		Outcome:                      "Success",
		InstitutionID:                inst.ID,
	}

	client := GetPharosClient(t)
	resp := client.PremisEventSave(event)
	require.Nil(t, resp.Error)
	savedEvent := resp.PremisEvent()
	require.NotNil(t, savedEvent)
	assert.Equal(t, event.Identifier, savedEvent.Identifier)
	assert.Equal(t, event.EventType, savedEvent.EventType)
	assert.NotEqual(t, 0, savedEvent.ID)
}

func TestPharosStorageRecordList(t *testing.T) {
	// From fixture data
	client := GetPharosClient(t)
	resp := client.StorageRecordList("institution1.edu/photos/picture1")
	require.Nil(t, resp.Error)
	records := resp.StorageRecords()
	require.Equal(t, 2, len(records))
	assert.Equal(t, "https://localhost:9899/preservation-va/25452f41-1b18-47b7-b334-751dfd5d011e", records[0].URL)
	assert.Equal(t, "https://localhost:9899/preservation-or/25452f41-1b18-47b7-b334-751dfd5d011e", records[1].URL)

	resp = client.StorageRecordList("institution2.edu/chocolate/picture1")
	require.Nil(t, resp.Error)
	records = resp.StorageRecords()
	require.Equal(t, 2, len(records))
	assert.Equal(t, "https://localhost:9899/preservation-va/3ba064ae-6a12-49e9-b9f8-cd63fbb173ce", records[0].URL)
	assert.Equal(t, "https://localhost:9899/preservation-or/3ba064ae-6a12-49e9-b9f8-cd63fbb173ce", records[1].URL)
}

func TestPharosStorageRecordCreate(t *testing.T) {
	client := GetPharosClient(t)
	sr := &registry.StorageRecord{
		URL: "https://example.com/inserted-by-int-test-01",
	}
	resp := client.StorageRecordSave(sr, "institution1.edu/photos/picture2")
	require.Nil(t, resp.Error)
	savedRecord := resp.StorageRecord()
	require.NotNil(t, savedRecord)
	require.NotEqual(t, 0, savedRecord.ID)
	require.NotEqual(t, 0, savedRecord.GenericFileID)
	require.Equal(t, "https://example.com/inserted-by-int-test-01", savedRecord.URL)
}

func TestPharosStorageRecordDelete(t *testing.T) {
	client := GetPharosClient(t)
	sr := &registry.StorageRecord{
		URL: "https://example.com/inserted-by-int-test-02",
	}
	resp := client.StorageRecordSave(sr, "institution1.edu/photos/picture2")
	require.Nil(t, resp.Error)
	savedRecord := resp.StorageRecord()
	require.NotNil(t, savedRecord)

	resp = client.StorageRecordDelete(savedRecord.ID)
	require.Nil(t, resp.Error)
	require.Equal(t, 204, resp.Response.StatusCode)
}

func TestWorkItemGet(t *testing.T) {
	LoadPharosFixtures(t)
	// ETag comes from fixture data
	etag := "01010101010101010101"
	item := GetWorkItem(t, etag)

	client := GetPharosClient(t)
	resp := client.WorkItemGet(item.ID)
	require.Nil(t, resp.Error)
	retrievedItem := resp.WorkItem()
	require.NotNil(t, retrievedItem)
	assert.Equal(t, item.ID, retrievedItem.ID)
	assert.Equal(t, item.ETag, retrievedItem.ETag)
	assert.Equal(t, item.Action, retrievedItem.Action)
}

func TestWorkItemList(t *testing.T) {
	LoadPharosFixtures(t)
	// Value from fixtures
	etag := "02020202020202020202"

	client := GetPharosClient(t)
	v := url.Values{}
	v.Add("per_page", "50")
	v.Add("etag", etag)
	resp := client.WorkItemList(v)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	items := resp.WorkItems()
	assert.Equal(t, 1, len(items))

	v.Del("etag")
	v.Add("stage", constants.StageReceive)
	resp = client.WorkItemList(v)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	items = resp.WorkItems()
	assert.Equal(t, 20, len(items))

	v.Set("stage", constants.StageCleanup)
	resp = client.WorkItemList(v)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	items = resp.WorkItems()
	assert.Equal(t, 6, len(items))
}

func TestWorkItemSaveAndFinish(t *testing.T) {
	LoadPharosFixtures(t)
	inst := GetInstitution(t, "institution2.edu")
	item := &registry.WorkItem{
		Name:          "fake_bag_15.tar",
		ETag:          "15151515151515151515",
		Bucket:        "aptrust.receiving.institution2.edu",
		User:          "system@aptrust.org",
		Note:          "This is a cancelled test item",
		Action:        constants.ActionRestore,
		Stage:         constants.StageRequested,
		Status:        constants.StatusCancelled,
		Outcome:       "This is a test item",
		BagDate:       testutil.Bloomsday,
		Date:          testutil.Bloomsday,
		Retry:         false,
		Pid:           0,
		InstitutionID: inst.ID,
	}
	client := GetPharosClient(t)
	resp := client.WorkItemSave(item)
	require.Nil(t, resp.Error)
	savedItem := resp.WorkItem()
	require.NotNil(t, savedItem)
	assert.NotEqual(t, 0, savedItem.ID)
	assert.Equal(t, item.ETag, savedItem.ETag)
	assert.Equal(t, item.Action, savedItem.Action)

	// As long as we have a restoration work item,
	// test this method as well.
	resp = client.FinishRestorationSpotTest(savedItem.ID)
	require.Nil(t, resp.Error)
	finishedItem := resp.WorkItem()
	require.NotNil(t, finishedItem)
	assert.Equal(t, savedItem.ID, finishedItem.ID)
	assert.True(t, strings.Contains(finishedItem.Note, "Email sent to admins"))
}

func TestBuildURL(t *testing.T) {
	relativeUrl := "/api/v2/blah/dir%2Ffile.pdf?param=value"
	expected := "http://localhost:9292/api/v2/blah/dir%2Ffile.pdf?param=value"
	client := GetPharosClient(t)
	assert.Equal(t, expected, client.BuildURL(relativeUrl))
}

func TestNewJSONRequest(t *testing.T) {
	client := GetPharosClient(t)
	postData := []byte(`{"key":"value"}`)
	req, err := client.NewJSONRequest("GET", "https://example.com", bytes.NewBuffer(postData))
	require.Nil(t, err)
	require.NotNil(t, req)

	assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
	assert.Equal(t, "application/json", req.Header.Get("Accept"))
	assert.Equal(t, client.APIUser, req.Header.Get("X-Pharos-API-User"))
	assert.Equal(t, client.APIKey, req.Header.Get("X-Pharos-API-Key"))
	assert.Equal(t, "Keep-Alive", req.Header.Get("Connection"))

	assert.Equal(t, "https", req.URL.Scheme)
	assert.Equal(t, "example.com", req.URL.Host)
	assert.Equal(t, "https://example.com", req.URL.Opaque)
}
