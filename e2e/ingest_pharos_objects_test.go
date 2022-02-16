//go:build e2e
// +build e2e

package e2e_test

import (
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Make sure that all expected IntellectualObjects with expected
// atributes are in Registry.
func testRegistryObjects() {
	for _, expectedObj := range ctx.ExpectedObjects {
		testObject(expectedObj)
	}
}

func testObject(expectedObj *registry.IntellectualObject) {
	resp := ctx.Context.RegistryClient.IntellectualObjectByIdentifier(expectedObj.Identifier)
	require.Nil(ctx.T, resp.Error)
	RegistryObj := resp.IntellectualObject()
	require.NotNil(ctx.T, RegistryObj, "Registry is missing %s", expectedObj.Identifier)
	testObjAgainstExpected(RegistryObj, expectedObj)
}

func testObjAgainstExpected(RegistryObj, expectedObj *registry.IntellectualObject) {
	t := ctx.T
	assert.Equal(t, RegistryObj.Title, expectedObj.Title, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.Description, expectedObj.Description, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.Identifier, expectedObj.Identifier, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.AltIdentifier, expectedObj.AltIdentifier, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.Access, expectedObj.Access, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.BagName, expectedObj.BagName, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.State, expectedObj.State, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.BagGroupIdentifier, expectedObj.BagGroupIdentifier, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.StorageOption, expectedObj.StorageOption, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.BagItProfileIdentifier, expectedObj.BagItProfileIdentifier, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.SourceOrganization, expectedObj.SourceOrganization, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.InternalSenderIdentifier, expectedObj.InternalSenderIdentifier, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.InternalSenderDescription, expectedObj.InternalSenderDescription, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.FileCount, expectedObj.FileCount, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.FileSize, expectedObj.FileSize, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.Institution, expectedObj.Institution, expectedObj.Identifier)
}
