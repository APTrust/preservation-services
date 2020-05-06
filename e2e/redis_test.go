// +build e2e

package e2e_test

import (
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test that interim processing data was deleted
func TestRedisRecords(t *testing.T) {
	context := common.NewContext()
	items := GetWorkItems(t, context)
	for _, item := range items {
		require.NotEmpty(t, item.ObjectIdentifier, item.ID)

		// IngestObject should have been deleted.
		ingestObject, _ := context.RedisClient.IngestObjectGet(
			item.ID, item.ObjectIdentifier)
		assert.Nil(t, ingestObject)

		// All IngestFiles should have been deleted.
		files, offset, err := context.RedisClient.GetBatchOfFileKeys(
			item.ID, uint64(0), int64(20))
		assert.Empty(t, files)
		assert.EqualValues(t, 0, offset)

		// TODO: Check work results. Should they be there or not?
	}
}
