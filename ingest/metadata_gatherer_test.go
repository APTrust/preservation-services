package ingest_test

import (
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	//"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewMetadataGatherer(t *testing.T) {
	context := common.NewContext()
	g := ingest.NewMetadataGatherer(context)
	require.NotNil(t, g)
	assert.Equal(t, context, g.Context)
}
