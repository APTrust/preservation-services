package registry_test

import (
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var cs = &registry.Checksum{
	Algorithm:     constants.AlgSha256,
	CreatedAt:     testutil.Bloomsday,
	DateTime:      testutil.Bloomsday,
	Digest:        "12345678",
	GenericFileID: 999,
	ID:            5432,
	UpdatedAt:     testutil.Bloomsday,
}

var csJson = `{"id":5432,"algorithm":"sha256","digest":"12345678","datetime":"1904-06-16T15:04:05Z","generic_file_id":999,"created_at":"1904-06-16T15:04:05Z","updated_at":"1904-06-16T15:04:05Z","generic_file_identifier":"","intellectual_object_id":0,"institution_id":0}`

func TestChecksumFromJson(t *testing.T) {
	checksum, err := registry.ChecksumFromJSON([]byte(csJson))
	require.Nil(t, err)
	assert.Equal(t, cs, checksum)
}

func TestChecksumToJson(t *testing.T) {
	actualJson, err := cs.ToJSON()
	require.Nil(t, err)
	assert.Equal(t, csJson, string(actualJson))
}
