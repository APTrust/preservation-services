// +build e2e

package e2e_test

import (
	"fmt"
	// "path"
	"strings"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/e2e"
	//"github.com/APTrust/preservation-services/models/registry"
	//"github.com/APTrust/preservation-services/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testFileRestorations() {
	for _, testFile := range e2e.FilesToRestore {
		objInfo, err := ctx.Context.S3StatObject(
			constants.StorageProviderAWS,
			ctx.TestInstitution.RestoreBucket,
			testFile.Identifier,
		)
		require.Nil(ctx.T, err, testFile.Identifier)
		require.NotNil(ctx.T, objInfo, testFile.Identifier)
		assert.EqualValues(ctx.T, testFile.Size, objInfo.Size)

		objIdentifier := objIdentFromFileIdent(testFile.Identifier)
		testWorkItemAfterRestore(objIdentifier, testFile.Identifier)
	}
}

func testBagRestorations() {
	for _, objIdentifier := range e2e.BagsToRestore {
		objInfo, err := ctx.Context.S3StatObject(
			constants.StorageProviderAWS,
			ctx.TestInstitution.RestoreBucket,
			fmt.Sprintf("%s.tar", objIdentifier),
		)
		require.Nil(ctx.T, err, objIdentifier)
		require.NotNil(ctx.T, objInfo, objIdentifier)

		testWorkItemAfterRestore(objIdentifier, "")
		validateBag(objIdentifier)
	}
}

func testWorkItemAfterRestore(objIdentifier, gfIdentifier string) {
	workItems := getRestoreWorkItems(objIdentifier, gfIdentifier)
	assert.Equal(ctx.T, 1, len(workItems))

	// Make sure WorkItem is resolved successful...
	assert.Equal(ctx.T, constants.StageResolve, workItems[0].Stage)
	assert.Equal(ctx.T, constants.StatusSuccess, workItems[0].Status)

	// Make sure note points to location of restored file
	assert.True(ctx.T, strings.Contains(workItems[0].Note, "https://"))
	if gfIdentifier != "" {
		assert.True(ctx.T, strings.Contains(workItems[0].Note, gfIdentifier))
	} else {
		assert.True(ctx.T, strings.Contains(workItems[0].Note, objIdentifier+".tar"))
	}
}

func validateBag(objIdentifier string) {
	// *******************************************************************
	// TODO: Check that bag is valid.
	//       See, e.g., ingest/bag_validation_test/TestBag_WithFetchTxt.
	//       Will need to make some private methods in that package
	//       public.
	// TODO: Ensure that bag has all of the Pharos object's active files
	//       After running validator, check validator.IngestObject and
	//       RedisClient.GetBatchOfFileKeys. Check the identifiers on
	//       all the returned ingest file objects.
	// *******************************************************************

	// profileName := constants.BagItProfileDefault
	// if strings.Contains(objIdentifier, "btr") {
	// 	profileName = constants.BagItProfileBTR
	// }
	// tarFileName := strings.Split(objIdentifier, "/")[1] + ".tar"
	// pathToBag := path.Join(ctx.Context.Config.BaseWorkingDir, "minio", "aptrust.restore.test.test.edu", tarFileName)
}
