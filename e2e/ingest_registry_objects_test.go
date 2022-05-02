//go:build e2e
// +build e2e

package e2e_test

import (
	"strings"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Make sure that all expected IntellectualObjects with expected
// atributes are in Registry.
func testRegistryObjects() {
	ctx.Context.Logger.Info("Testing Registry objects")
	for _, expectedObj := range ctx.ExpectedObjects {
		ctx.Context.Logger.Infof("Testing Registry object %s", expectedObj.Identifier)
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

	// Special - Part of https://trello.com/c/k14P6teL/924-filter-by-btr-bagit-profile-not-working
	// BTR test bags contain bad BTR profile identifier from DART.
	// These identifiers should be corrected during ingest to match constants.BTRProfileIdentifier.
	if strings.Contains(RegistryObj.Identifier, "btr") {
		assert.Equal(t, RegistryObj.BagItProfileIdentifier, constants.BTRProfileIdentifier, expectedObj.Identifier)
	} else {
		assert.Equal(t, RegistryObj.BagItProfileIdentifier, expectedObj.BagItProfileIdentifier, expectedObj.Identifier)
	}

	assert.Equal(t, RegistryObj.SourceOrganization, expectedObj.SourceOrganization, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.InternalSenderIdentifier, expectedObj.InternalSenderIdentifier, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.InternalSenderDescription, expectedObj.InternalSenderDescription, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.FileCount, expectedObj.FileCount, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.Size, expectedObj.Size, expectedObj.Identifier)
	assert.Equal(t, RegistryObj.InstitutionID, expectedObj.InstitutionID, expectedObj.Identifier)
}
