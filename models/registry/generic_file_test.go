package registry_test

import (
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var genericFile = &registry.GenericFile{
	Checksums: []*registry.Checksum{
		{
			Algorithm:     "md5",
			DateTime:      testutil.Bloomsday,
			Digest:        "1234",
			GenericFileID: 5432,
		},
		{
			Algorithm:     "sha256",
			DateTime:      testutil.Bloomsday,
			Digest:        "5678",
			GenericFileID: 5432,
		},
	},
	CreatedAt:                    testutil.Bloomsday,
	FileFormat:                   "text/html",
	FileModified:                 testutil.Bloomsday,
	ID:                           5432,
	Identifier:                   "test.edu.bag/data/index.html",
	InstitutionID:                9355,
	IntellectualObjectID:         1000,
	IntellectualObjectIdentifier: "test.edu.bag",
	LastFixityCheck:              testutil.Bloomsday,
	PremisEvents: []*registry.PremisEvent{
		{
			Agent:                        "Maxwell Smart",
			CreatedAt:                    testutil.Bloomsday,
			DateTime:                     testutil.Bloomsday,
			Detail:                       "detail?",
			EventType:                    "accession",
			GenericFileID:                5432,
			GenericFileIdentifier:        "test.edu.bag/data/index.html",
			Identifier:                   "you you eye dee",
			InstitutionID:                9355,
			IntellectualObjectID:         1000,
			IntellectualObjectIdentifier: "test.edu.bag",
			Object:                       "scissors",
			OutcomeDetail:                "just fine",
			OutcomeInformation:           "fine I say",
			Outcome:                      "stop asking",
			UpdatedAt:                    testutil.Bloomsday,
		},
	},
	Size:          int64(8900),
	State:         "A",
	StorageOption: constants.StorageStandard,
	StorageRecords: []*registry.StorageRecord{
		{URL: "https://example.com/preservation/1234"},
		{URL: "https://example.com/replication/1234"},
	},
	URI:       "https://s3.example.com/preservation/5432",
	UpdatedAt: testutil.Bloomsday,
}

var gfJson = `{"checksums":[{"algorithm":"md5","created_at":"0001-01-01T00:00:00Z","datetime":"1904-06-16T15:04:05Z","digest":"1234","generic_file_id":5432,"updated_at":"0001-01-01T00:00:00Z"},{"algorithm":"sha256","created_at":"0001-01-01T00:00:00Z","datetime":"1904-06-16T15:04:05Z","digest":"5678","generic_file_id":5432,"updated_at":"0001-01-01T00:00:00Z"}],"created_at":"1904-06-16T15:04:05Z","file_format":"text/html","file_modified":"1904-06-16T15:04:05Z","id":5432,"identifier":"test.edu.bag/data/index.html","institution_id":9355,"intellectual_object_id":1000,"intellectual_object_identifier":"test.edu.bag","last_fixity_check":"1904-06-16T15:04:05Z","premis_events":[{"agent":"Maxwell Smart","created_at":"1904-06-16T15:04:05Z","date_time":"1904-06-16T15:04:05Z","detail":"detail?","event_type":"accession","generic_file_id":5432,"generic_file_identifier":"test.edu.bag/data/index.html","identifier":"you you eye dee","institution_id":9355,"intellectual_object_id":1000,"intellectual_object_identifier":"test.edu.bag","object":"scissors","outcome_detail":"just fine","outcome_information":"fine I say","outcome":"stop asking","updated_at":"1904-06-16T15:04:05Z"}],"size":8900,"state":"A","storage_option":"Standard","storage_records":[{"generic_file_id":0,"url":"https://example.com/preservation/1234"},{"generic_file_id":0,"url":"https://example.com/replication/1234"}],"uri":"https://s3.example.com/preservation/5432","updated_at":"1904-06-16T15:04:05Z"}`

// JSON format for Pharos post/put is {"generic_file": <object>}
// Also note that we don't serialize fields that Pharos doesn't accept.
var gfJsonForPharos = `{"generic_file":{"checksums_attributes":[{"algorithm":"md5","created_at":"0001-01-01T00:00:00Z","datetime":"1904-06-16T15:04:05Z","digest":"1234","generic_file_id":5432,"updated_at":"0001-01-01T00:00:00Z"},{"algorithm":"sha256","created_at":"0001-01-01T00:00:00Z","datetime":"1904-06-16T15:04:05Z","digest":"5678","generic_file_id":5432,"updated_at":"0001-01-01T00:00:00Z"}],"file_format":"text/html","id":5432,"identifier":"test.edu.bag/data/index.html","institution_id":9355,"intellectual_object_id":1000,"premis_events_attributes":[{"agent":"Maxwell Smart","created_at":"1904-06-16T15:04:05Z","date_time":"1904-06-16T15:04:05Z","detail":"detail?","event_type":"accession","generic_file_id":5432,"generic_file_identifier":"test.edu.bag/data/index.html","identifier":"you you eye dee","institution_id":9355,"intellectual_object_id":1000,"intellectual_object_identifier":"test.edu.bag","object":"scissors","outcome_detail":"just fine","outcome_information":"fine I say","outcome":"stop asking","updated_at":"1904-06-16T15:04:05Z"}],"size":8900,"storage_option":"Standard","storage_records_attributes":[{"generic_file_id":0,"url":"https://example.com/preservation/1234"},{"generic_file_id":0,"url":"https://example.com/replication/1234"}],"uri":"https://s3.example.com/preservation/5432"}}`

func TestGenericFileFromJson(t *testing.T) {
	gf, err := registry.GenericFileFromJSON([]byte(gfJson))
	require.Nil(t, err)
	assert.Equal(t, genericFile, gf)
}

func TestGenericFileToJson(t *testing.T) {
	actualJson, err := genericFile.ToJSON()
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

func TestPathInBag(t *testing.T) {
	gf := &registry.GenericFile{
		Identifier:                   "test.edu/sample-bag/data/file.txt",
		IntellectualObjectIdentifier: "test.edu/sample-bag",
	}
	assert.Equal(t, "data/file.txt", gf.PathInBag())
}

func TestPathIsTagFile(t *testing.T) {

	// Not a tag file because it's in the payload directory
	gf := &registry.GenericFile{
		Identifier:                   "test.edu/sample-bag/data/file.txt",
		IntellectualObjectIdentifier: "test.edu/sample-bag",
	}
	assert.False(t, gf.IsTagFile())

	// Manifest is not a tag file
	gf.Identifier = "test.edu/sample-bag/manifest-sha256.txt"
	assert.False(t, gf.IsTagFile())

	// Tag manifest is not a tag file
	gf.Identifier = "test.edu/sample-bag/tagmanifest-sha256.txt"
	assert.False(t, gf.IsTagFile())

	// This is a tag file because it's not in the payload dir
	// and it's not a manifest or tag manifest
	gf.Identifier = "test.edu/sample-bag/bag-info.txt"
	assert.True(t, gf.IsTagFile())
}
