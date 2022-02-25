package registry_test

import (
	"testing"
	"time"

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
	CreatedAt:            testutil.Bloomsday,
	FileFormat:           "text/html",
	FileModified:         testutil.Bloomsday,
	ID:                   5432,
	Identifier:           "test.edu/bag/data/index.html",
	InstitutionID:        9355,
	IntellectualObjectID: 1000,
	LastFixityCheck:      testutil.Bloomsday,
	PremisEvents: []*registry.PremisEvent{
		{
			Agent:                        "Maxwell Smart",
			CreatedAt:                    testutil.Bloomsday,
			DateTime:                     testutil.Bloomsday,
			Detail:                       "detail?",
			EventType:                    "accession",
			GenericFileID:                5432,
			GenericFileIdentifier:        "test.edu/bag/data/index.html",
			Identifier:                   "you you eye dee",
			InstitutionID:                9355,
			IntellectualObjectID:         1000,
			IntellectualObjectIdentifier: "test.edu/bag",
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
		{URL: "https://example.com/preservation/76038bae-48f9-487b-8579-bcc48d70e64f"},
		{URL: "https://example.com/replication/76038bae-48f9-487b-8579-bcc48d70e64f"},
	},
	UUID:      "76038bae-48f9-487b-8579-bcc48d70e64f",
	UpdatedAt: testutil.Bloomsday,
}

var gfJson = `{"checksums":[{"algorithm":"md5","digest":"1234","datetime":"1904-06-16T15:04:05Z","generic_file_id":5432,"created_at":"0001-01-01T00:00:00Z","updated_at":"0001-01-01T00:00:00Z","generic_file_identifier":"","intellectual_object_id":0,"institution_id":0},{"algorithm":"sha256","digest":"5678","datetime":"1904-06-16T15:04:05Z","generic_file_id":5432,"created_at":"0001-01-01T00:00:00Z","updated_at":"0001-01-01T00:00:00Z","generic_file_identifier":"","intellectual_object_id":0,"institution_id":0}],"created_at":"1904-06-16T15:04:05Z","file_format":"text/html","file_modified":"1904-06-16T15:04:05Z","id":5432,"identifier":"test.edu/bag/data/index.html","institution_id":9355,"intellectual_object_id":1000,"last_fixity_check":"1904-06-16T15:04:05Z","premis_events":[{"agent":"Maxwell Smart","created_at":"1904-06-16T15:04:05Z","date_time":"1904-06-16T15:04:05Z","detail":"detail?","event_type":"accession","generic_file_id":5432,"generic_file_identifier":"test.edu/bag/data/index.html","identifier":"you you eye dee","institution_id":9355,"intellectual_object_id":1000,"intellectual_object_identifier":"test.edu/bag","object":"scissors","outcome_detail":"just fine","outcome_information":"fine I say","outcome":"stop asking","updated_at":"1904-06-16T15:04:05Z"}],"size":8900,"state":"A","storage_option":"Standard","storage_records":[{"generic_file_id":0,"url":"https://example.com/preservation/76038bae-48f9-487b-8579-bcc48d70e64f"},{"generic_file_id":0,"url":"https://example.com/replication/76038bae-48f9-487b-8579-bcc48d70e64f"}],"uuid":"76038bae-48f9-487b-8579-bcc48d70e64f","updated_at":"1904-06-16T15:04:05Z"}`

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

func TestGenericFileUUID(t *testing.T) {
	assert.Equal(t, "76038bae-48f9-487b-8579-bcc48d70e64f", genericFile.UUID)
}

func TestGetLatestChecksum(t *testing.T) {
	gf := &registry.GenericFile{
		Checksums: []*registry.Checksum{
			{
				Algorithm:     "md5",
				DateTime:      testutil.Bloomsday,
				Digest:        "old-md5",
				GenericFileID: 5432,
			},
			{
				Algorithm:     "sha256",
				DateTime:      testutil.Bloomsday,
				Digest:        "old-sha256",
				GenericFileID: 5432,
			},
			{
				Algorithm:     "md5",
				DateTime:      time.Now().UTC(),
				Digest:        "new-md5",
				GenericFileID: 5432,
			},
			{
				Algorithm:     "sha256",
				DateTime:      time.Now().UTC(),
				Digest:        "new-sha256",
				GenericFileID: 5432,
			},
		},
	}
	assert.Equal(t, "new-md5", gf.GetLatestChecksum("md5").Digest)
	assert.Equal(t, "new-sha256", gf.GetLatestChecksum("sha256").Digest)
}

func TestPathInBag(t *testing.T) {
	gf := &registry.GenericFile{
		Identifier: "test.edu/sample-bag/data/file.txt",
	}
	p, err := gf.PathInBag()
	require.Nil(t, err)
	assert.Equal(t, "data/file.txt", p)
}

func TestPathMinusInstitution(t *testing.T) {
	gf := &registry.GenericFile{
		Identifier: "test.edu/sample-bag/data/file.txt",
	}
	p, err := gf.PathMinusInstitution()
	require.Nil(t, err)
	assert.Equal(t, "sample-bag/data/file.txt", p)
}

func TestInstitutionIdentifier(t *testing.T) {
	gf := &registry.GenericFile{
		Identifier: "test.edu/sample-bag/data/file.txt",
	}
	assert.Equal(t, "test.edu", gf.InstitutionIdentifier())
}

func TestPathIsTagFile(t *testing.T) {

	// Not a tag file because it's in the payload directory
	gf := &registry.GenericFile{
		Identifier: "test.edu/sample-bag/data/file.txt",
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

func TestGFIntellectualObjectIdentifier(t *testing.T) {
	gf := &registry.GenericFile{
		Identifier: "test.edu/photos/data/image1.png",
	}
	objIdentifier, err := gf.IntellectualObjectIdentifier()
	require.Nil(t, err)
	assert.Equal(t, "test.edu/photos", objIdentifier)
}
