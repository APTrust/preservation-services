// -- go:build e2e

package e2e_test

import (
	"net/url"

	"github.com/APTrust/preservation-services/models/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testGenericFiles() {
	t := ctx.T
	registryFiles := getGenericFiles()
	for _, expectedFile := range ctx.ExpectedFiles {
		registryFile := findFile(registryFiles, expectedFile.Identifier)
		require.NotNil(t, registryFile, "Not in Registry: %s", expectedFile.Identifier)
		testGenericFile(registryFile, expectedFile)
	}
}

func testGenericFile(registryFile, expectedFile *registry.GenericFile) {
	testFileAttributes(registryFile, expectedFile)
	testChecksums(registryFile, expectedFile)
	testStorageRecords(registryFile, expectedFile)
	testPremisEvents(registryFile, expectedFile)
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

func testFileAttributes(registryFile, expectedFile *registry.GenericFile) {
	t := ctx.T
	expectedObjIdentifier, err := expectedFile.IntellectualObjectIdentifier()
	require.Nil(t, err)
	actualObjIdentifier, err := registryFile.IntellectualObjectIdentifier()
	require.Nil(t, err)

	assert.Equal(t, expectedFile.Identifier, registryFile.Identifier, registryFile.Identifier)
	assert.Equal(t, expectedFile.FileFormat, registryFile.FileFormat, registryFile.Identifier)
	assert.Equal(t, expectedObjIdentifier, actualObjIdentifier, registryFile.Identifier)
	assert.Equal(t, expectedFile.Size, registryFile.Size, registryFile.Identifier)
	assert.Equal(t, expectedFile.State, registryFile.State, registryFile.Identifier)
	assert.Equal(t, expectedFile.StorageOption, registryFile.StorageOption, registryFile.Identifier)
}
