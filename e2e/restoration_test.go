// -- go:build e2e

package e2e_test

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/e2e"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testFileRestorations() {
	ctx.Context.Logger.Infof("Starting test of %d restoration files", len(e2e.FilesToRestore))
	for _, testFile := range e2e.FilesToRestore {
		ctx.Context.Logger.Infof("Testing restoration file %s", testFile.Identifier)
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

// All we're really testing here is that the bag is present
// and contains all the expected files.
func validateBag(objIdentifier string) {
	intelObj := getObject(objIdentifier)
	registryFiles := getregistryFiles(objIdentifier)
	tarFileName := strings.Split(objIdentifier, "/")[1] + ".tar"
	pathToBag := path.Join(ctx.Context.Config.BaseWorkingDir, "minio", "aptrust.restore.test.test.edu", "test.edu", tarFileName)

	// Parse the restored bag and find out which ingest files are in it.
	ingestFiles, err := scanBag(intelObj, pathToBag)
	require.Nil(ctx.T, err, objIdentifier)
	assert.True(ctx.T, len(ingestFiles) > 0, objIdentifier)

	// DEBUG
	for _, f := range ingestFiles {
		fmt.Println(f.Identifier())
	}
	// END DEBUG

	for _, gf := range registryFiles {

		// Make sure file was restored with bag
		restoredFile := ingestFiles[gf.Identifier]
		require.NotNil(ctx.T, restoredFile, gf.Identifier)

		RegistryLatestSha256 := gf.GetLatestChecksum(constants.AlgSha256)
		require.NotNil(ctx.T, RegistryLatestSha256, gf.Identifier)
		restoredFileSha256 := restoredFile.GetChecksum(constants.SourceIngest, constants.AlgSha256)
		require.NotNil(ctx.T, restoredFileSha256, gf.Identifier)

		// Make sure the restored version was the LATEST version
		assert.Equal(ctx.T, RegistryLatestSha256.Digest, restoredFileSha256.Digest, gf.Identifier)
	}
}

func getObject(objIdentifier string) *registry.IntellectualObject {
	resp := ctx.Context.RegistryClient.IntellectualObjectByIdentifier(objIdentifier)
	require.Nil(ctx.T, resp.Error, objIdentifier)
	intelObj := resp.IntellectualObject()
	require.NotNil(ctx.T, intelObj, objIdentifier)
	return intelObj
}

// Get a list of files belonging to this object from Registry
func getregistryFiles(objIdentifier string) []*registry.GenericFile {
	params := url.Values{}
	params.Set("intellectual_object_identifier", objIdentifier)
	params.Set("include_relations", "true")
	params.Set("include_storage_records", "true")
	params.Set("page", "1")
	params.Set("per_page", "200")
	resp := ctx.Context.RegistryClient.GenericFileList(params)
	require.Nil(ctx.T, resp.Error, objIdentifier)
	require.NotEmpty(ctx.T, resp.GenericFiles())
	return resp.GenericFiles()
}

func scanBag(intelObj *registry.IntellectualObject, pathToBag string) (map[string]*service.IngestFile, error) {
	ingestFiles := make(map[string]*service.IngestFile)

	ingestObject := &service.IngestObject{
		S3Key:         intelObj.BagName + ".tar",
		ID:            intelObj.ID,
		Institution:   ctx.TestInstitution.Identifier,
		InstitutionID: intelObj.InstitutionID,
		StorageOption: intelObj.StorageOption,
	}

	tarredBag, err := os.Open(pathToBag)
	if err != nil {
		return ingestFiles, err
	}

	defer tarredBag.Close()
	scanner := ingest.NewTarredBagScanner(
		tarredBag,
		ingestObject,
		ctx.Context.Config.IngestTempDir)

	for {
		ingestFile, err := scanner.ProcessNextEntry()
		// EOF expected at end of file
		if err == io.EOF {
			break
		}
		// Any non-EOF error is a problem
		if err != nil {
			return ingestFiles, err
		}
		// ProcessNextEntry returns nil for directories,
		// symlinks, and anything else that's not a file.
		// We can't store these non-objects in S3, so we
		// ignore them.
		if ingestFile == nil {
			continue
		}
		ingestFiles[ingestFile.Identifier()] = ingestFile
	}
	return ingestFiles, nil
}
