// +build e2e

package e2e_test

import (
	"testing"

	"github.com/APTrust/preservation-services/e2e"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
)

type E2ECtx struct {
	Context         *common.Context
	T               *testing.T
	ExpectedObjects []*registry.IntellectualObject
	ExpectedFiles   []*registry.GenericFile
	TestInstitution *registry.Institution
	ReingestFiles   []string
}

var ctx E2ECtx

// TestEndToEnd runs a number of bags through ingest and reingest,
// then tests that:
//
// * all Pharos data is complete and as expected
// * all files are in the correct preservation storage buckets
//   with complete metadata
// * all interim processing data was deleted from S3 staging and Redis
func TestEndToEnd(t *testing.T) {
	initTestContext(t)
	testIngest()
	testRestoration()
}

func testIngest() {
	// First, push through ingest bags...
	pushBagsToReceiving(e2e.InitialBags())
	waitForInitialIngestCompletion()

	// Then reingest some bags...
	pushBagsToReceiving(e2e.ReingestBags())
	waitForReingestCompletion()

	// Test that all objects, files, checksums, storage records
	// and premis events from these ingests are as expected
	testPharosObjects()
	testGenericFiles()

	// Make sure we cleaned up all interim processing resources.
	testS3Cleanup(ctx.Context.Config.StagingBucket)
	testS3Cleanup(ctx.TestInstitution.ReceivingBucket)
	testRedisCleanup()
}

func testRestoration() {
	// TODO: Write this
}
