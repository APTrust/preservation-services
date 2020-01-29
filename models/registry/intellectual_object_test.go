package registry_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

var obj = &registry.IntellectualObject{
	Access:                 constants.AccessConsortia,
	AltIdentifier:          "alt-1234",
	BagGroupIdentifier:     "group-1",
	BagItProfileIdentifier: "https://example.com/profile.json",
	BagName:                "BagOfTricks",
	CreatedAt:              testutil.Bloomsday,
	Description:            "Bag of tricks",
	ETag:                   "987654",
	FileCount:              388,
	FileSize:               int64(400000),
	Id:                     28,
	Identifier:             "test.edu/BagOfTricks",
	Institution:            "test.edu",
	InstitutionId:          301,
	SourceOrganization:     "Test University",
	State:                  "A",
	StorageOption:          constants.StorageWasabiOR,
	Title:                  "Thirteen Ways of Looking at a Blackbird",
	UpdatedAt:              testutil.Bloomsday,
}

var objJson = `{"access":"consortia","alt_identifier":"alt-1234","bag_group_identifier":"group-1","bagit_profile_identifier":"https://example.com/profile.json","bag_name":"BagOfTricks","created_at":"1904-06-16T15:04:05Z","description":"Bag of tricks","etag":"987654","file_count":388,"file_size":400000,"id":28,"identifier":"test.edu/BagOfTricks","institution":"test.edu","institution_id":301,"source_organization":"Test University","state":"A","storage_option":"Wasabi-OR","title":"Thirteen Ways of Looking at a Blackbird","updated_at":"1904-06-16T15:04:05Z"}`

func TestIntellectualObjectFromJson(t *testing.T) {
	intelObj, err := registry.IntellectualObjectFromJson(objJson)
	require.Nil(t, err)
	assert.Equal(t, obj, intelObj)
}

func TestIntellectualObjectToJson(t *testing.T) {
	actualJson, err := obj.ToJson()
	require.Nil(t, err)
	assert.Equal(t, objJson, actualJson)
}
