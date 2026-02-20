//go:build e2e
// +build e2e

package e2e_test

import (
	"fmt"
	"net/url"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/APTrust/preservation-services/e2e"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
//   - all Registry data is complete and as expected
//   - all files are in the correct preservation storage buckets
//     with complete metadata
//   - all interim processing data was deleted from S3 staging and Redis
func TestEndToEnd(t *testing.T) {
	initTestContext(t)
	testIngest()
	testRestoration()
	testFixityChecks()
	testDeletions()

	// Run this last, as it adds items to Registry
	// that the tests above won't expect to be present.
	testReingestStorageOptions(t)
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
func testDeletions() {
	createDeletionWorkItems()
	waitForDeletionCompletion()
	testFileDeletions()
	testObjectDeletions()
}

// Test fix for the "reingest with different storage option bug":
// https://trello.com/c/C4XlgSNU and https://trello.com/c/ccxvAQkv
//
// Description:
//
// 1. Ingest a bag into standard storage.
// 2. Ingest the same bag into Glacier-OH.
// 3. Ingest the same bag into Wasabi-VA.
// 4. Ensure that all of the bag's files are in standard storage only.
//
// Storage option coercion should ensure that all files
// always go into the same storage option as the initial ingest.
// This is to prevent us collecting different versions of
// each file in different storage locations. (E.g. first version
// in standard, second in Glacier-OH, third in Wasabi-VA.)
//
// This policy also prevents high storage charges and billing headaches.
func testReingestStorageOptions(t *testing.T) {
	fileName := "reingest-bug-test.tar"
	dirs := []string{
		"01-Standard",
		"02-Glacier-OH",
		"03-Wasabi-VA",
	}
	testbags := make([]*e2e.TestBag, 0)
	for _, dir := range dirs {
		pathToBag := path.Join(testutil.PathToTestData(), "reingest", dir, fileName)
		testbags = append(testbags, &e2e.TestBag{PathToBag: pathToBag})
	}

	// Make sure we ingest the standard bag first.
	pushBagsToReceiving(testbags[0:1])
	waitForCompletedIngest(t, fileName, 1)

	// Now re-ingest the next two, in any order,
	// and wait again.
	pushBagsToReceiving(testbags[1:2])
	waitForCompletedIngest(t, fileName, 3)

	// We want to look at the files that were ingested,
	// but to do that, we have to get the object ID first.
	params := url.Values{}
	params.Set("identifier", "test.edu/reingest-bug-test")
	params.Set("page", "1")
	params.Set("per_page", "1")
	resp := ctx.Context.RegistryClient.IntellectualObjectByIdentifier("test.edu/reingest-bug-test")
	require.Nil(t, resp.Error)
	obj := resp.IntellectualObject()

	params = url.Values{}
	params.Set("intellectual_object_id", fmt.Sprintf("%d", obj.ID))
	params.Set("page", "1")
	params.Set("per_page", "30")
	resp = ctx.Context.RegistryClient.GenericFileList(params)
	require.Nil(t, resp.Error)
	files := resp.GenericFiles()

	assert.Equal(t, 3, len(files))

	// Check storage records in the detailed GenericFile
	// record. There should be two, and both should point
	// to our standard storage buckets. There should be
	// no Glacier-OH record or Wasabi-VA record.
	for _, file := range files {
		resp = ctx.Context.RegistryClient.GenericFileByID(file.ID)
		require.Nil(t, resp.Error)
		gf := resp.GenericFile()
		require.Equal(t, 2, len(gf.StorageRecords))
		for _, sr := range gf.StorageRecords {
			fmt.Println(sr.URL)
			isStandardStorage := strings.Contains(sr.URL, ctx.Context.Config.BucketStandardOR) || strings.Contains(sr.URL, ctx.Context.Config.BucketStandardVA)
			assert.True(t, isStandardStorage, sr.URL)
		}
	}
}

func waitForCompletedIngest(t *testing.T, tarFileName string, howMany int) {
	requestCount := 0
	params := url.Values{}
	params.Set("name", tarFileName)
	params.Set("action", "Ingest")
	params.Set("status", "Success")
	params.Set("page", "1")
	params.Set("per_page", "20")
	for {
		resp := ctx.Context.RegistryClient.WorkItemList(params)
		require.Nil(t, resp.Error)
		if resp.Count >= howMany {
			return
		}
		requestCount += 1
		if requestCount > 20 {
			return
		}
		time.Sleep(5 * time.Second)
	}
}
