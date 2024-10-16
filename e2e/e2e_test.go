//go:build e2e
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
// * all Registry data is complete and as expected
// * all files are in the correct preservation storage buckets
//   with complete metadata
// * all interim processing data was deleted from S3 staging and Redis
func TestEndToEnd(t *testing.T) {
	initTestContext(t)
	testIngest()
	testRestoration()
	testFixityChecks()
	testDeletions()
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
	testRegistryObjects()
	testGenericFiles()

	// Make sure we cleaned up all interim processing resources.
	testS3Cleanup(ctx.Context.Config.StagingBucket)
	testS3Cleanup(ctx.TestInstitution.ReceivingBucket)
	testRedisCleanup()
}

func testRestoration() {
	createRestorationWorkItems()
	waitForRestorationCompletion()
	testFileRestorations()
	testBagRestorations()
}

func testFixityChecks() {
	queueFixityItems()
	waitForFixityCompletion()
	testFixityResults()
}

// Can't automate deletion tests because they require
// email confirmations. We may try to work around this
// later, but it's a substantial amount of work, and
// we already test deletions in our integration tests.
//
func testDeletions() {
	createDeletionWorkItems()
	waitForDeletionCompletion()
	testFileDeletions()
	testObjectDeletions()
}
