// +build integration

package network_test

import (
	"encoding/json"
	"fmt"
	// "github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/network"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"testing"
	//"time"
)

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

func getPharosClient(t *testing.T) *network.PharosClient {
	config := common.NewConfig()
	assert.Equal(t, "test", config.ConfigName)
	client, err := network.NewPharosClient(
		config.PharosURL,
		config.PharosAPIVersion,
		config.PharosAPIUser,
		config.PharosAPIKey,
	)
	require.Nil(t, err)
	require.NotNil(t, client)
	return client
}

func GetInstitution(t *testing.T, identifier string) *registry.Institution {
	client := getPharosClient(t)
	resp := client.InstitutionGet(identifier)
	assert.NotNil(t, resp)
	require.Nil(t, resp.Error)
	institution := resp.Institution()
	require.NotNil(t, institution)
	return institution
}

func TestEscapeFileIdentifier(t *testing.T) {
	assert.Equal(t,
		"test.edu%2Fobj%2Ffile%20name%3F.txt",
		network.EscapeFileIdentifier("test.edu/obj/file name?.txt"))
}

func TestPharosInstitutionGet(t *testing.T) {
	LoadPharosFixtures(t)
	client := getPharosClient(t)
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
	client := getPharosClient(t)
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
	assert.Equal(t, len(InstFixtures), len(institutions))
	// Make sure we got the expected items in our list of 4
	for _, inst := range institutions {
		assert.NotNil(t, InstFixtures[inst.Identifier])
	}
}

func TestPharosIntellectualObjectGet(t *testing.T) {
	LoadPharosFixtures(t)
	client := getPharosClient(t)
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
	}
}

func TestPharosIntellectualObjectList(t *testing.T) {
	LoadPharosFixtures(t)
	client := getPharosClient(t)
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
	assert.Equal(t, len(ObjectFixtures), len(objects))
	// Make sure we got the expected items in our list of 4
	for _, obj := range objects {
		assert.NotNil(t, ObjectFixtures[obj.Identifier])
	}
}

func TestPharosIntellectualObjectSave_Update(t *testing.T) {
	LoadPharosFixtures(t)
	client := getPharosClient(t)

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
	}
}

func TestPharosIntellectualObjectSave_Create(t *testing.T) {
	intelObj := testutil.GetIntellectualObject()

	// Make sure we're using an institution id that was
	// loaded with the test fixtures
	testInst := GetInstitution(t, "test.edu")
	intelObj.InstitutionId = testInst.Id

	// Id of zero means it's never been saved.
	require.Equal(t, 0, intelObj.Id)

	client := getPharosClient(t)
	resp := client.IntellectualObjectSave(intelObj)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	assert.Equal(t,
		"/api/v2/objects/test.edu",
		resp.Request.URL.Opaque)
	obj := resp.IntellectualObject()
	require.NotNil(t, obj)
	assert.Equal(t, intelObj.Identifier, obj.Identifier)
	assert.NotEqual(t, 0, obj.Id)
	assert.NotEqual(t, intelObj.UpdatedAt, obj.UpdatedAt)

}
