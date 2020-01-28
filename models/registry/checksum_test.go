package registry_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

var cs = &registry.Checksum{
	Algorithm:     constants.AlgSha256,
	CreatedAt:     testutil.Bloomsday,
	DateTime:      testutil.Bloomsday,
	Digest:        "12345678",
	GenericFileId: 999,
	Id:            5432,
	UpdatedAt:     testutil.Bloomsday,
}

var csJson = `{"algorithm":"sha256","created_at":"1904-06-16T15:04:05Z","datetime":"1904-06-16T15:04:05Z","digest":"12345678","generic_file_id":999,"id":5432,"updated_at":"1904-06-16T15:04:05Z"}`

func TestChecksumFromJson(t *testing.T) {
	checksum, err := registry.ChecksumFromJson(csJson)
	require.Nil(t, err)
	assert.Equal(t, cs, checksum)
}

func TestChecksumToJson(t *testing.T) {
	actualJson, err := cs.ToJson()
	require.Nil(t, err)
	assert.Equal(t, csJson, actualJson)
}
