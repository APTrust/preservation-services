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

func LoadInstitutionFixtures(t *testing.T) map[string]*registry.Institution {
	data, err := testutil.ReadPharosFixture("institutions.json")
	require.Nil(t, err)
	institutions := make(map[string]*registry.Institution)
	err = json.Unmarshal(data, &institutions)
	require.Nil(t, err)
	return institutions
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
	instFixtures := LoadInstitutionFixtures(t)
	client := getPharosClient(t)
	for _, inst := range instFixtures {
		resp := client.InstitutionGet(inst.Identifier)
		assert.NotNil(t, resp)
		require.Nil(t, resp.Error)
		institution := resp.Institution()
		assert.NotNil(t, institution)
		assert.Equal(t, inst.Identifier, institution.Identifier)
	}
}

func TestPharosInstitutionList(t *testing.T) {
	instFixtures := LoadInstitutionFixtures(t)
	client := getPharosClient(t)
	v := url.Values{}
	v.Add("order", "name")
	v.Add("per_page", "20")
	resp := client.InstitutionList(v)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	institutions := resp.Institutions()
	assert.Equal(t, len(instFixtures), len(institutions))
	// Make sure we got the expected items in our list of 4
	for _, inst := range institutions {
		assert.NotNil(t, instFixtures[inst.Identifier])
	}
}
