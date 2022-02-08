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
	//	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/network"
	"github.com/APTrust/preservation-services/util/logger"
	//	"github.com/APTrust/preservation-services/util/testutil"
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
		resp := client.InstitutionById(int64(i))
		assert.NotNil(t, resp)
		require.Nil(t, resp.Error)
		assert.Equal(t,
			fmt.Sprintf("/admin-api/v3/institutions/show/%d", i),
			resp.Request.URL.Opaque)
		institution := resp.Institution()
		assert.NotNil(t, institution)
		assert.Equal(t, i, institution.ID)
	}
}

func TestRegistryInstitutionList(t *testing.T) {
	client := GetRegistryClient(t)
	v := url.Values{}
	v.Add("order", "name")
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
	v.Add("order", "identifier")
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
