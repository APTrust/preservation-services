//go:build e2e
// +build e2e

package e2e_test

import (
	"strings"

	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Our JSON file doesn't list expected storage records, but we know
// what buckets each file should be in, based on the StorageOption.
// Note that file URLs will change every time we run the tests, because
// the URLs end with UUIDs.
func testStorageRecords(registryFile, expectedFile *registry.GenericFile) {
	t := ctx.T
	require.Equal(t, len(expectedFile.StorageRecords), len(registryFile.StorageRecords))

	hasURLFor := make(map[string]bool)
	buckets := ctx.Context.Config.PreservationBucketsFor(expectedFile.StorageOption)
	for _, b := range buckets {
		hasURLFor[b.Bucket] = false
	}
	for _, sr := range registryFile.StorageRecords {
		assert.True(t, strings.HasPrefix(sr.URL, "https://"))
		assert.True(t, util.LooksLikeUUID(registryFile.UUID), registryFile.Identifier)
		for _, b := range buckets {
			if strings.Contains(sr.URL, b.Bucket) {
				hasURLFor[b.Bucket] = true
			}
		}

		// Test that the file that the StorageRecord points to
		// is actually present in S3, with correct metadata.
		testS3File(sr, registryFile)
	}
	for _, b := range buckets {
		assert.True(t, hasURLFor[b.Bucket], "File %s missing URL for preservation bucket %s", expectedFile.Identifier, b.Bucket)
	}
}
