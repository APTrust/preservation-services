//go:build e2e
// +build e2e

package e2e_test

import (
	"net/url"

	"github.com/APTrust/preservation-services/models/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testGenericFiles() {
	t := ctx.T
	pharosFiles := getGenericFiles()
	for _, expectedFile := range ctx.ExpectedFiles {
		pharosFile := findFile(pharosFiles, expectedFile.Identifier)
		require.NotNil(t, pharosFile, "Not in Pharos: %s", expectedFile.Identifier)
		testGenericFile(pharosFile, expectedFile)
	}
}

func testGenericFile(pharosFile, expectedFile *registry.GenericFile) {
	testFileAttributes(pharosFile, expectedFile)
	testChecksums(pharosFile, expectedFile)
	testStorageRecords(pharosFile, expectedFile)
	testPremisEvents(pharosFile, expectedFile)
}

func findFile(files []*registry.GenericFile, identifier string) *registry.GenericFile {
	for _, f := range files {
		if f.Identifier == identifier {
			return f
		}
	}
	return nil
}

func getGenericFiles() []*registry.GenericFile {
	params := url.Values{}
	params.Set("institution_identifier", ctx.TestInstitution.Identifier)
	params.Set("include_relations", "true")
	params.Set("include_storage_records", "true")
	params.Set("page", "1")
	params.Set("per_page", "200")
	resp := ctx.Context.RegistryClient.GenericFileList(params)
	require.Nil(ctx.T, resp.Error)
	return resp.GenericFiles()
}

func testFileAttributes(pharosFile, expectedFile *registry.GenericFile) {
	t := ctx.T
	assert.Equal(t, pharosFile.Identifier, expectedFile.Identifier, expectedFile.Identifier)
	assert.Equal(t, pharosFile.FileFormat, expectedFile.FileFormat, expectedFile.Identifier)
	assert.Equal(t, pharosFile.IntellectualObjectIdentifier, expectedFile.IntellectualObjectIdentifier, expectedFile.Identifier)
	assert.Equal(t, pharosFile.Size, expectedFile.Size, expectedFile.Identifier)
	assert.Equal(t, pharosFile.State, expectedFile.State, expectedFile.Identifier)
	assert.Equal(t, pharosFile.StorageOption, expectedFile.StorageOption, expectedFile.Identifier)
}
