//go:build e2e
// +build e2e

package e2e_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Make sure the latest checksums in Registry match the latest checksums in our
// JSON file of expected data. Reingested files will have two versions of each
// checksum (md5, sha256, etc.). We want to make sure the latest one is present
// and correct.
func testChecksums(registryFile, expectedFile *registry.GenericFile) {
	t := ctx.T
	for _, alg := range constants.SupportedManifestAlgorithms {
		// Match latest digests
		expected := expectedFile.GetLatestChecksum(alg)
		require.NotNil(t, expected, "Missing JSON checksum for %s -> %s", expectedFile.Identifier, alg)
		actual := registryFile.GetLatestChecksum(alg)
		require.NotNil(t, actual, "Missing Registry checksum for %s -> %s", expectedFile.Identifier, alg)
		assert.Equal(t, expected.Digest, actual.Digest, "%s -> %s", expectedFile.Identifier, expected.Algorithm)
	}

	// Once-ingest files have 4 checksums, twice-ingested have 8.
	// Make sure we get what's expected.
	assert.Equal(t, len(expectedFile.Checksums), len(registryFile.Checksums), expectedFile.Identifier)
}
