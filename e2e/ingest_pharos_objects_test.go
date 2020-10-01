// +build e2e

package e2e_test

import (
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Make sure that all expected IntellectualObjects with expected
// atributes are in Pharos.
func testPharosObjects() {
	for _, expectedObj := range ctx.ExpectedObjects {
		testObject(expectedObj)
	}
}

func testObject(expectedObj *registry.IntellectualObject) {
	resp := ctx.Context.PharosClient.IntellectualObjectGet(expectedObj.Identifier)
	require.Nil(ctx.T, resp.Error)
	pharosObj := resp.IntellectualObject()
	require.NotNil(ctx.T, pharosObj, "Pharos is missing %s", expectedObj.Identifier)
	testObjAgainstExpected(pharosObj, expectedObj)
}

func testObjAgainstExpected(pharosObj, expectedObj *registry.IntellectualObject) {
	t := ctx.T
	assert.Equal(t, pharosObj.Title, expectedObj.Title, expectedObj.Identifier)
	assert.Equal(t, pharosObj.Description, expectedObj.Description, expectedObj.Identifier)
	assert.Equal(t, pharosObj.Identifier, expectedObj.Identifier, expectedObj.Identifier)
	assert.Equal(t, pharosObj.AltIdentifier, expectedObj.AltIdentifier, expectedObj.Identifier)
	assert.Equal(t, pharosObj.Access, expectedObj.Access, expectedObj.Identifier)
	assert.Equal(t, pharosObj.BagName, expectedObj.BagName, expectedObj.Identifier)
	assert.Equal(t, pharosObj.State, expectedObj.State, expectedObj.Identifier)
	assert.Equal(t, pharosObj.BagGroupIdentifier, expectedObj.BagGroupIdentifier, expectedObj.Identifier)
	assert.Equal(t, pharosObj.StorageOption, expectedObj.StorageOption, expectedObj.Identifier)
	assert.Equal(t, pharosObj.BagItProfileIdentifier, expectedObj.BagItProfileIdentifier, expectedObj.Identifier)
	assert.Equal(t, pharosObj.SourceOrganization, expectedObj.SourceOrganization, expectedObj.Identifier)
	assert.Equal(t, pharosObj.InternalSenderIdentifier, expectedObj.InternalSenderIdentifier, expectedObj.Identifier)
	assert.Equal(t, pharosObj.InternalSenderDescription, expectedObj.InternalSenderDescription, expectedObj.Identifier)
	assert.Equal(t, pharosObj.FileCount, expectedObj.FileCount, expectedObj.Identifier)
	assert.Equal(t, pharosObj.FileSize, expectedObj.FileSize, expectedObj.Identifier)
	assert.Equal(t, pharosObj.Institution, expectedObj.Institution, expectedObj.Identifier)
}
