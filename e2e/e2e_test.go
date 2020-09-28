// +build e2e

package e2e_test

import (
	"testing"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/e2e"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type E2ECtx struct {
	Context *common.Context
	T       *testing.T
}

var ctx E2ECtx

func TestEndToEnd(t *testing.T) {
	context := common.NewContext()
	ctx = E2ECtx{
		Context: context,
		T:       t,
	}
	pushBagsToReceiving(e2e.InitialBags())
	waitForInitialIngestCompletion()
	pushBagsToReceiving(e2e.ReingestBags())
	waitForReingestCompletion()
}

func pushBagsToReceiving(testbags []*e2e.TestBag) {
	client := ctx.Context.S3Clients[constants.StorageProviderAWS]
	for _, tb := range testbags {
		_, err := client.FPutObject(
			"aptrust.receiving.test.test.edu",
			tb.TarFileName(),
			tb.PathToBag,
			minio.PutObjectOptions{},
		)
		require.Nil(ctx.T, err)
		ctx.Context.Logger.Infof("Copied %s to receiving bucket", tb.PathToBag)
	}
}

func waitForInitialIngestCompletion() {
	for {
		if initialIngestsComplete() {
			break
		}
		time.Sleep(10 * time.Second)
	}
}

func waitForReingestCompletion() {
	for {
		if reingestsComplete() {
			break
		}
		time.Sleep(10 * time.Second)
	}
}

func testBagCount(includeInvalid, includeReingest bool) uint64 {
	count := uint64(0)
	for _, tb := range e2e.TestBags {
		if (includeInvalid || tb.IsValidBag) && (includeReingest || !tb.IsUpdate) {
			count++
		}
	}
	return count
}

func initialIngestsComplete() bool {
	return ingestsComplete(testBagCount(false, false))
}

func reingestsComplete() bool {
	return ingestsComplete(testBagCount(false, true))
}

func ingestsComplete(count uint64) bool {
	require.True(ctx.T, count > 0)
	stats, err := ctx.Context.NSQClient.GetStats()
	require.Nil(ctx.T, err)
	channelName := constants.IngestCleanup + "_worker_chan"
	summary, err := stats.GetChannelSummary(constants.IngestCleanup, channelName)
	require.Nil(ctx.T, err)
	ctx.Context.Logger.Infof("In %s: %d in flight, %d finished. Want %d", channelName, summary.InFlightCount, summary.FinishCount, count)
	return summary.InFlightCount == 0 && summary.FinishCount == count
}

func testPharosObjects() {
	objects, err := e2e.LoadObjectJSON()
	require.Nil(ctx.T, err)
	for _, expectedObj := range objects {
		testObject(expectedObj)
	}
}

func testObject(expectedObj *registry.IntellectualObject) {
	resp := ctx.Context.PharosClient.IntellectualObjectGet(expectedObj.Identifier)
	require.Nil(ctx.T, resp.Error)
	pharosObj := resp.IntellectualObject()
	require.NotNil(ctx.T, pharosObj, "Pharos is missing %s", expectedObj.Identifier)
	testObjAgainstExpected(pharosObj, expectedObj)
	testGenericFiles(expectedObj)
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
	assert.Equal(t, pharosObj.ETag, expectedObj.ETag, expectedObj.Identifier)
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

func testGenericFiles(expectedObj *registry.IntellectualObject) {
	t := ctx.T
	files, err := e2e.LoadGenericFileJSON()
	require.Nil(t, err)
	require.NotEmpty(t, files)
	objFiles, err := e2e.GetFilesByObjectIdentifier(files, expectedObj.Identifier)
	require.Nil(t, err)
	require.NotEmpty(t, objFiles)
	for _, expectedFile := range objFiles {
		resp := ctx.Context.PharosClient.GenericFileGet(expectedFile.Identifier)
		require.Nil(t, resp.Error, expectedFile.Identifier)
		pharosFile := resp.GenericFile()
		require.NotNil(t, pharosFile, expectedFile.Identifier)
		testFileAttributes(pharosFile, expectedFile)
		testChecksums(pharosFile, expectedFile)
		testStorageRecords(pharosFile, expectedFile)
		testPremisEvents(pharosFile, expectedFile)
	}
}

func testFileAttributes(pharosFile, expectedFile *registry.GenericFile) {

}

func testChecksums(pharosFile, expectedFile *registry.GenericFile) {

}

func testStorageRecords(pharosFile, expectedFile *registry.GenericFile) {

}

func testPremisEvents(pharosFile, expectedFile *registry.GenericFile) {

}

func testWorkItemsAfterIngest() {

}
