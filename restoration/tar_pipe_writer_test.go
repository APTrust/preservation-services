package restoration_test

import (
	"archive/tar"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/restoration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTarPipeWriter(t *testing.T) {
	w := restoration.NewTarPipeWriter()
	require.NotNil(t, w)
	assert.NotNil(t, w.GetReader())
}

func TestGetManifestHashes(t *testing.T) {
	w := restoration.NewTarPipeWriter()
	allSupported := w.GetManifestHashes(constants.SupportedManifestAlgorithms)
	assert.Equal(t, len(constants.SupportedManifestAlgorithms), len(allSupported))
	for _, hash := range allSupported {
		assert.NotNil(t, hash)
	}

	aptrustAlgs := w.GetManifestHashes(constants.APTrustRestorationAlgorithms)
	assert.Equal(t, len(constants.APTrustRestorationAlgorithms), len(aptrustAlgs))
	for _, hash := range aptrustAlgs {
		assert.NotNil(t, hash)
	}
}

func TestEnsureDirectoryEntry(t *testing.T) {
	dirname := "path/to/some/directory/"
	w := restoration.NewTarPipeWriter()
	require.NotNil(t, w)
	reader := w.GetReader()

	// Test that EnsureDirectory actually writes the tar header for the
	// directory entry. We need to read from the PipeReader in a separate
	// go routine that won't block. The call to io.Copy will hang until
	// we close the PipeWriter in the call to w.Finish().

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		buf := new(strings.Builder)
		io.Copy(buf, reader)
		assert.Equal(t, 1, strings.Count(buf.String(), dirname))
		wg.Done()
	}()

	// EnsureDirectoryEntry should write each directory tar header
	// ONLY ONCE. We'll test that two calls produce only one header.

	err := w.EnsureDirectoryEntry(dirname)
	require.Nil(t, err)

	err = w.EnsureDirectoryEntry(dirname)
	require.Nil(t, err)

	w.Finish()

	wg.Wait()
}

func TestValidateHeader(t *testing.T) {
	w := restoration.NewTarPipeWriter()
	require.NotNil(t, w)

	missingName := &tar.Header{
		Size: 1234,
	}
	assert.NotNil(t, w.ValidateHeader(missingName))

	badSize := &tar.Header{
		Name: "some-name",
		Size: -1,
	}
	assert.NotNil(t, w.ValidateHeader(badSize))

	ok := &tar.Header{
		Name: "some-name",
		Size: 300,
	}
	assert.Nil(t, w.ValidateHeader(ok))
}

func TestGetReader(t *testing.T) {
	w := restoration.NewTarPipeWriter()
	require.NotNil(t, w)
	require.NotNil(t, w.GetReader())
}

func TestAddFile(t *testing.T) {
	w := restoration.NewTarPipeWriter()
	require.NotNil(t, w)
	pipeReader := w.GetReader()

	tarHeader := &tar.Header{
		Name: "SampleData.txt",
		Size: 11,
	}
	stringReader := strings.NewReader("sample data")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		buf := new(strings.Builder)
		io.Copy(buf, pipeReader)

		// Make sure the header appears
		assert.Equal(t, 1, strings.Count(buf.String(), "SampleData.txt"))

		// Make sure the content appears
		assert.Equal(t, 1, strings.Count(buf.String(), "sample data"))

		wg.Done()
	}()

	// Write an entry into the tar archive
	digests, err := w.AddFile(tarHeader, stringReader, constants.SupportedManifestAlgorithms)

	// Close PipeWriter, or we'll hang forever
	w.Finish()

	// Wait for reads and writes to complete
	wg.Wait()

	// Now test
	require.Nil(t, err)
	require.Equal(t, len(constants.SupportedManifestAlgorithms), len(digests))
	for _, digest := range digests {
		assert.True(t, len(digest) >= 32)
	}
}
