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

func TestEndToEnd(t *testing.T) {
	initTestContext(t)
	pushBagsToReceiving(e2e.InitialBags())
	waitForInitialIngestCompletion()
	pushBagsToReceiving(e2e.ReingestBags())
	waitForReingestCompletion()

	// Test that all objects, files, checksums, storage records
	// and premis events from these ingests are as expected
	testPharosObjects()
	testGenericFiles()

	// TODO: Test cleanup of staging bucket and redis.
	// TODO: Test e2e_ingest_post_test and e2e_reingest_post_test
	//       queue depth instead of finished items in cleanup queue.
}
