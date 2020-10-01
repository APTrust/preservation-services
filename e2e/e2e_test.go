// +build e2e

package e2e_test

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/e2e"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util"
	"github.com/minio/minio-go/v6"
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

}

// Set up a context for testing.
func initTestContext(t *testing.T) {
	objects, err := e2e.LoadObjectJSON()
	require.Nil(t, err)

	files, err := e2e.LoadGenericFileJSON()
	require.Nil(t, err)

	context := common.NewContext()
	ctx = E2ECtx{
		Context:         context,
		T:               t,
		ExpectedObjects: objects,
		ExpectedFiles:   files,
	}
	ctx.TestInstitution = GetInstitution("test.edu")
}

// Push some bags into the receiving bucket using Minio.
// We do this instead of a simple filesystem copy because
// Pharos WorkItems use ETags to distinguish between versions
// of a bag. Minio creates ETags, file copying doesn't.
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

// Check NSQ every 10 seconds to see whether all initial ingests
// are complete.
func waitForInitialIngestCompletion() {
	for {
		if initialIngestsComplete() {
			break
		}
		time.Sleep(10 * time.Second)
	}
}

// Check NSQ every 10 seconds to see whether all reingests
// are complete.
func waitForReingestCompletion() {
	for {
		if reingestsComplete() {
			break
		}
		time.Sleep(10 * time.Second)
	}
}

// This returns the number of bags expected to be ingested
// or reingested. The reingest count includes all ingests
// plus reingests.
func testBagCount(includeInvalid, includeReingest bool) uint64 {
	count := uint64(0)
	for _, tb := range e2e.TestBags {
		if (includeInvalid || tb.IsValidBag) && (includeReingest || !tb.IsUpdate) {
			count++
		}
	}
	return count
}

// Returns true if the initial version of our test bags have
// been ingested.
func initialIngestsComplete() bool {
	return ingestsComplete(testBagCount(false, false))
}

// Returns true if the updated versions of our test bags have
// been ingested.
func reingestsComplete() bool {
	return ingestsComplete(testBagCount(false, true))
}

// This queries NSQ to find the number of finished items in a channel.
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

func testWorkItemsAfterIngest() {
	t := ctx.T
	params := url.Values{}
	params.Set("item_action", constants.ActionIngest)
	params.Set("institution_id", strconv.Itoa(ctx.TestInstitution.ID))
	resp := ctx.Context.PharosClient.WorkItemList(params)
	require.Nil(t, resp.Error)
	pharosItems := resp.WorkItems()
	require.NotEmpty(t, pharosItems)

	itemCounts := make(map[string]int)

	// 17 ingests plus 4 reingests
	assert.Equal(t, 21, len(pharosItems))
	for _, item := range pharosItems {
		assert.Equal(t, "Finished cleanup. Ingest complete.", item.Note)
		assert.Equal(t, constants.StageCleanup, item.Stage)
		assert.Equal(t, constants.StatusSuccess, item.Status)
		assert.Equal(t, "Ingest complete", item.Outcome)
		assert.False(t, item.BagDate.IsZero())
		assert.False(t, item.Date.IsZero())
		assert.False(t, item.QueuedAt.IsZero())
		assert.NotEmpty(t, item.ObjectIdentifier)
		assert.Empty(t, item.GenericFileIdentifier)
		assert.Empty(t, item.Node)
		assert.Equal(t, 0, item.Pid)
		assert.NotEmpty(t, item.InstitutionID)
		assert.NotEmpty(t, item.Size)
		assert.False(t, item.NeedsAdminReview)

		if _, ok := itemCounts[item.Name]; !ok {
			itemCounts[item.Name] = 0
		}
		itemCounts[item.Name]++
	}

	for _, bag := range e2e.ReingestBags() {
		count := itemCounts[bag.TarFileName()]
		assert.NotNil(t, count)
		assert.Equal(t, 2, count)
	}
}

func testGenericFiles() {
	t := ctx.T
	pharosFiles := getGenericFiles()
	for _, expectedFile := range ctx.ExpectedFiles {
		pharosFile := findFile(pharosFiles, expectedFile.Identifier)
		require.NotNil(t, pharosFile, "Not in Pharos: %s", expectedFile.Identifier)
		testGenericFile(pharosFile, expectedFile)
	}
}

func testGenericFile(pharosFile, expectedFile *registry.GenericFile) {
	testFileAttributes(pharosFile, expectedFile)
	testChecksums(pharosFile, expectedFile)
	testStorageRecords(pharosFile, expectedFile)
	testPremisEvents(pharosFile, expectedFile)
}

func findFile(files []*registry.GenericFile, identifier string) *registry.GenericFile {
	for _, f := range files {
		if f.Identifier == identifier {
			return f
		}
	}
	return nil
}

func getGenericFiles() []*registry.GenericFile {
	params := url.Values{}
	params.Set("institution_identifier", ctx.TestInstitution.Identifier)
	params.Set("include_relations", "true")
	params.Set("include_storage_records", "true")
	params.Set("page", "1")
	params.Set("per_page", "200")
	resp := ctx.Context.PharosClient.GenericFileList(params)
	require.Nil(ctx.T, resp.Error)
	return resp.GenericFiles()
}

func testFileAttributes(pharosFile, expectedFile *registry.GenericFile) {
	t := ctx.T
	assert.Equal(t, pharosFile.Identifier, expectedFile.Identifier, expectedFile.Identifier)
	assert.Equal(t, pharosFile.FileFormat, expectedFile.FileFormat, expectedFile.Identifier)
	assert.Equal(t, pharosFile.IntellectualObjectIdentifier, expectedFile.IntellectualObjectIdentifier, expectedFile.Identifier)
	assert.Equal(t, pharosFile.Size, expectedFile.Size, expectedFile.Identifier)
	assert.Equal(t, pharosFile.State, expectedFile.State, expectedFile.Identifier)
	assert.Equal(t, pharosFile.StorageOption, expectedFile.StorageOption, expectedFile.Identifier)
}

// Make sure the latest checksums in Pharos match the latest checksums in our
// JSON file of expected data. Reingested files will have two versions of each
// checksum (md5, sha256, etc.). We want to make sure the latest one is present
// and correct.
func testChecksums(pharosFile, expectedFile *registry.GenericFile) {
	t := ctx.T
	for _, alg := range constants.SupportedManifestAlgorithms {
		// Match latest digests
		expected := expectedFile.GetLatestChecksum(alg)
		require.NotNil(t, expected, "Missing JSON checksum for %s -> %s", expectedFile.Identifier, alg)
		actual := pharosFile.GetLatestChecksum(alg)
		require.NotNil(t, actual, "Missing Pharos checksum for %s -> %s", expectedFile.Identifier, alg)
		assert.Equal(t, expected.Digest, actual.Digest, "%s -> %s", expectedFile.Identifier, expected.Algorithm)
	}

	// Once-ingest files have 4 checksums, twice-ingested have 8.
	// Make sure we get what's expected.
	assert.Equal(t, len(expectedFile.Checksums), len(pharosFile.Checksums), expectedFile.Identifier)
}

// Our JSON file doesn't list expected storage records, but we know
// what buckets each file should be in, based on the StorageOption.
// Note that file URLs will change every time we run the tests, because
// the URLs end with UUIDs.
func testStorageRecords(pharosFile, expectedFile *registry.GenericFile) {
	t := ctx.T
	require.Equal(t, len(expectedFile.StorageRecords), len(pharosFile.StorageRecords))

	hasURLFor := make(map[string]bool)
	buckets := ctx.Context.Config.PreservationBucketsFor(expectedFile.StorageOption)
	for _, b := range buckets {
		hasURLFor[b.Bucket] = false
	}
	for _, sr := range pharosFile.StorageRecords {
		assert.True(t, strings.HasPrefix(sr.URL, "https://"))
		assert.True(t, util.LooksLikeUUID(pharosFile.UUID()), pharosFile.Identifier)
		for _, b := range buckets {
			if strings.Contains(sr.URL, b.Bucket) {
				hasURLFor[b.Bucket] = true
			}
		}
	}
	for _, b := range buckets {
		assert.True(t, hasURLFor[b.Bucket], "File %s missing URL for preservation bucket %s", expectedFile.Identifier, b.Bucket)
	}
}

func testPremisEvents(pharosFile, expectedFile *registry.GenericFile) {
	assert.Equal(ctx.T, len(expectedFile.PremisEvents), len(pharosFile.PremisEvents))
	pharosEvents := hashEvents(pharosFile.PremisEvents)
	for _, event := range expectedFile.PremisEvents {
		key := eventKey(event)
		pharosEvent := pharosEvents[key]
		assert.NotNil(ctx.T, pharosEvent, "Pharos file %s is missing event %s", pharosFile.Identifier, key)
	}
}

// Use hash/map instead of repeated nested loop lookups
func hashEvents(events []*registry.PremisEvent) map[string]*registry.PremisEvent {
	eventMap := make(map[string]*registry.PremisEvent)
	for _, e := range events {
		eventMap[eventKey(e)] = e
	}
	return eventMap
}

// Unique key to match expected and actual events.
// Key must include type and outcome info, and must not include
// UUIDs that change on every ingest.
func eventKey(event *registry.PremisEvent) string {
	suffix := event.OutcomeDetail
	if event.EventType == constants.EventIdentifierAssignment || event.EventType == constants.EventReplication {
		suffix = event.OutcomeInformation
	}
	return fmt.Sprintf("%s / %s", event.EventType, suffix)
}

func GetInstitution(identifier string) *registry.Institution {
	resp := ctx.Context.PharosClient.InstitutionGet(identifier)
	assert.NotNil(ctx.T, resp)
	require.Nil(ctx.T, resp.Error)
	institution := resp.Institution()
	require.NotNil(ctx.T, institution)
	return institution
}
