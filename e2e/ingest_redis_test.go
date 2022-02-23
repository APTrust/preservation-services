// -- go:build e2e

package e2e_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// After all ingests are complete, the Redis interim data should be deleted.
func testRedisCleanup() {
	keys, err := ctx.Context.RedisClient.Keys("*")
	require.Nil(ctx.T, err)
	for _, key := range keys {
		assert.Empty(ctx.T, key, "WorkItem %s was not deleted from Redis", key)
	}
}
