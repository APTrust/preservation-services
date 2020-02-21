// +build integration

package network_test

import (
	"encoding/json"
	//"fmt"
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

func TestPharosInstitutionGet(t *testing.T) {
	LoadPharosFixtures(t)
	client := getPharosClient(t)
	for _, inst := range InstFixtures {
		resp := client.InstitutionGet(inst.Identifier)
		assert.NotNil(t, resp)
		require.Nil(t, resp.Error)
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
	institutions := resp.Institutions()
	assert.Equal(t, len(InstFixtures), len(institutions))
	// Make sure we got the expected items in our list of 4
	for _, inst := range institutions {
		assert.NotNil(t, InstFixtures[inst.Identifier])
	}
}
