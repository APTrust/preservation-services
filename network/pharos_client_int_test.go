// +build integration

package network_test

import (
	//"fmt"
	// "github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"testing"
	//"time"
)

// Pharos test fixtures are defined in the Pharos project.
// We may want to move these constants to a place where
// other test modules can access them.
const InstIdentifier = "test.edu"

var Institutions = []string{
	"aptrust.org",
	"institution1.edu",
	"institution2.edu",
	"test.edu",
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
	client := getPharosClient(t)
	resp := client.InstitutionGet(InstIdentifier)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	institution := resp.Institution()
	assert.NotNil(t, institution)
	assert.Equal(t, InstIdentifier, institution.Identifier)
}

func TestPharosInstitutionList(t *testing.T) {
	client := getPharosClient(t)
	v := url.Values{}
	v.Add("order", "name")
	v.Add("per_page", "20")
	resp := client.InstitutionList(v)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.Error)
	institutions := resp.Institutions()
	assert.Equal(t, 4, len(institutions))
	for i, inst := range institutions {
		assert.Equal(t, Institutions[i], inst.Identifier)
	}
}
