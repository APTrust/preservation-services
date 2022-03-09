package service_test

import (
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
)

func TestIngestChecksumToChecksum(t *testing.T) {
	ingestChecksum := &service.IngestChecksum{
		Algorithm: constants.AlgMd5,
		DateTime:  testutil.Bloomsday,
		Digest:    "12345",
		Source:    constants.SourceIngest,
	}
	cs := ingestChecksum.ToRegistryChecksum(9999)
	assert.Equal(t, ingestChecksum.Algorithm, cs.Algorithm)
	assert.Equal(t, ingestChecksum.DateTime, cs.DateTime)
	assert.Equal(t, ingestChecksum.Digest, cs.Digest)
	assert.EqualValues(t, 9999, cs.GenericFileID)
}
