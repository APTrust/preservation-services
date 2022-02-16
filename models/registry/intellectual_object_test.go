package registry_test

import (
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var obj = &registry.IntellectualObject{
	Access:                    constants.AccessConsortia,
	AltIdentifier:             "alt-1234",
	BagGroupIdentifier:        "group-1",
	BagItProfileIdentifier:    "https://example.com/profile.json",
	BagName:                   "BagOfTricks",
	CreatedAt:                 testutil.Bloomsday,
	Description:               "Bag of tricks",
	ETag:                      "987654",
	ID:                        28,
	Identifier:                "test.edu/BagOfTricks",
	Institution:               "test.edu",
	InstitutionID:             301,
	InternalSenderDescription: "int-sender-desc",
	InternalSenderIdentifier:  "int-sender-ident",
	SourceOrganization:        "Test University",
	State:                     "A",
	StorageOption:             constants.StorageWasabiOR,
	Title:                     "Thirteen Ways of Looking at a Blackbird",
	UpdatedAt:                 testutil.Bloomsday,
}

var objJson = `{"access":"consortia","alt_identifier":"alt-1234","bag_group_identifier":"group-1","bagit_profile_identifier":"https://example.com/profile.json","bag_name":"BagOfTricks","created_at":"1904-06-16T15:04:05Z","description":"Bag of tricks","etag":"987654","id":28,"identifier":"test.edu/BagOfTricks","internal_sender_description":"int-sender-desc","internal_sender_identifier":"int-sender-ident","institution":"test.edu","institution_id":301,"source_organization":"Test University","state":"A","storage_option":"Wasabi-OR","title":"Thirteen Ways of Looking at a Blackbird","updated_at":"1904-06-16T15:04:05Z"}`

func TestIntellectualObjectFromJson(t *testing.T) {
	intelObj, err := registry.IntellectualObjectFromJSON([]byte(objJson))
	require.Nil(t, err)
	assert.Equal(t, obj, intelObj)
}

func TestIntellectualObjectToJson(t *testing.T) {
	actualJson, err := obj.ToJSON()
	require.Nil(t, err)
	assert.Equal(t, objJson, string(actualJson))
}

func TestObjIdentifierMinusInstitution(t *testing.T) {
	obj := &registry.IntellectualObject{
		Identifier: "test.edu/sample-bag",
	}
	ident, err := obj.IdentifierMinusInstitution()
	require.Nil(t, err)
	assert.Equal(t, "sample-bag", ident)

	obj.Identifier = "sample-bag"
	ident, err = obj.IdentifierMinusInstitution()
	require.NotNil(t, err)
	assert.Equal(t, "", ident)
}
