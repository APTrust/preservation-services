package registry_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

var genericFile = &registry.GenericFile{
	CreatedAt:                    testutil.Bloomsday,
	FileFormat:                   "text/html",
	FileModified:                 testutil.Bloomsday,
	Id:                           5432,
	Identifier:                   "test.edu.bag/data/index.html",
	InstitutionId:                9355,
	IntellectualObjectId:         1000,
	IntellectualObjectIdentifier: "test.edu.bag",
	LastFixityCheck:              testutil.Bloomsday,
	Size:                         int64(8900),
	State:                        "A",
	StorageOption:                constants.StorageStandard,
	URI:                          "https://s3.example.com/preservation/5432",
	UpdatedAt:                    testutil.Bloomsday,
}

var gfJson = `{"created_at":"1904-06-16T15:04:05Z","file_format":"text/html","file_modified":"1904-06-16T15:04:05Z","id":5432,"identifier":"test.edu.bag/data/index.html","institution_id":9355,"intellectual_object_id":1000,"intellectual_object_identifier":"test.edu.bag","last_fixity_check":"1904-06-16T15:04:05Z","size":8900,"state":"A","storage_option":"Standard","uri":"https://s3.example.com/preservation/5432","updated_at":"1904-06-16T15:04:05Z"}`

// JSON format for Pharos post/put is {"generic_file": <object>}
// Also note that we don't serialize fields that Pharos doesn't accept.
var gfJsonForPharos = `{"generic_file":{"file_format":"text/html","id":5432,"identifier":"test.edu.bag/data/index.html","institution_id":9355,"intellectual_object_id":1000,"size":8900,"storage_option":"Standard","uri":"https://s3.example.com/preservation/5432"}}`

func TestGenericFileFromJson(t *testing.T) {
	gf, err := registry.GenericFileFromJson([]byte(gfJson))
	require.Nil(t, err)
	assert.Equal(t, genericFile, gf)
}

func TestGenericFileToJson(t *testing.T) {
	actualJson, err := genericFile.ToJson()
	require.Nil(t, err)
	assert.Equal(t, gfJson, string(actualJson))
}

func TestGenericFileSerializeForPharos(t *testing.T) {
	actualJson, err := genericFile.SerializeForPharos()
	require.Nil(t, err)
	assert.Equal(t, gfJsonForPharos, string(actualJson))
}

func TestGenericUUID(t *testing.T) {
	assert.Equal(t, "5432", genericFile.UUID())
}
