// +build e2e

package e2e_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/e2e"
	//"github.com/APTrust/preservation-services/models/registry"
	//"github.com/APTrust/preservation-services/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testFileRestorations() {
	// File is in restoration bucket
	// File has expected size and checksum
	// WorkItem is marked complete
	inst := getInstitution("test.edu")
	require.NotNil(ctx.T, inst)
	for _, testFile := range e2e.FilesToRestore {
		objInfo, err := ctx.Context.S3StatObject(
			constants.StorageProviderAWS,
			inst.RestoreBucket,
			testFile.Identifier,
		)
		require.Nil(ctx.T, err, testFile.Identifier)
		require.NotNil(ctx.T, objInfo, testFile.Identifier)
		assert.EqualValues(ctx.T, testFile.Size, objInfo.Size)
	}
}

func testBagRestorations() {
	// Bag is in restoration bucket
	// Bag has all expected files
	// Bag is valid
	// WorkItem is marked complete
}

func testWorkItemAfterRestore() {

}
