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
	InitialBags     []*e2e.TestBag
	ReingestBags    []*e2e.TestBag
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
}

// Set up a context for testing.
func initTestContext(t *testing.T) {
	objects, err := e2e.LoadObjectJSON()
	require.Nil(t, err)

	files, err := e2e.LoadGenericFileJSON()
	require.Nil(t, err)

	// Figure out which files in our test data are
	// reingests. Once-ingested files have four checksums;
	// twice-ingested files have eight.
	reingestFiles := make([]string, 0)
	for _, f := range files {
		if len(f.Checksums) == 4 {
			reingestFiles = append(reingestFiles, f.Identifier)
		}
	}

	context := common.NewContext()
	ctx = E2ECtx{
		Context:         context,
		T:               t,
		ExpectedObjects: objects,
		ExpectedFiles:   files,
		InitialBags:     e2e.InitialBags(),
		ReingestBags:    e2e.ReingestBags(),
		ReingestFiles:   reingestFiles,
	}
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

// This is the meat of the ingest test: Make sure that all
// expected objects, files, etc. are in Pharos.
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
	objFiles, err := e2e.GetFilesByObjectIdentifier(ctx.ExpectedFiles, expectedObj.Identifier)
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
	params := url.Values{}
	params.Set("generic_file_identifier", expectedFile.Identifier)
	resp := ctx.Context.PharosClient.ChecksumList(params)
	require.Nil(t, resp.Error)
	pharosChecksums := resp.Checksums()

	for _, alg := range constants.SupportedManifestAlgorithms {
		// Match latest digests
		expected, err := getLatestChecksum(expectedFile.Checksums, alg)
		require.Nil(t, err, "Missing JSON checksum for %s -> %s", expectedFile.Identifier, alg)
		actual, err := getLatestChecksum(pharosChecksums, alg)
		require.Nil(t, err, "Missing Pharos checksum for %s -> %s", expectedFile.Identifier, alg)
		assert.Equal(t, expected.Digest, actual.Digest, "%s -> %s", expectedFile.Identifier, expected.Algorithm)

		// Make sure reingests have expected number of checksums.
		expectedCount := checksumCount(expectedFile.Checksums, alg)
		actualCount := checksumCount(pharosChecksums, alg)
		assert.Equal(t, expectedCount, actualCount, "%s -> %s", expectedFile.Identifier, expected.Algorithm)
	}
}

// Our JSON file doesn't list expected storage records, but we know
// what buckets each file should be in, based on the StorageOption.
// Note that file URLs will change every time we run the tests, because
// the URLs end with UUIDs.
func testStorageRecords(pharosFile, expectedFile *registry.GenericFile) {
	t := ctx.T
	expectedCount := 1
	if expectedFile.StorageOption == constants.StorageStandard {
		expectedCount = 2
	}
	require.Equal(t, expectedCount, len(pharosFile.StorageRecords))

	foundInBucket := make(map[string]bool)
	buckets := ctx.Context.Config.PreservationBucketsFor(expectedFile.StorageOption)
	for _, b := range buckets {
		foundInBucket[b.Bucket] = false
	}
	for _, sr := range pharosFile.StorageRecords {
		assert.True(t, strings.HasPrefix(sr.URL, "https://"))
		assert.True(t, util.LooksLikeUUID(pharosFile.UUID()), pharosFile.Identifier)
		for _, b := range buckets {
			if strings.Contains(sr.URL, b.Bucket) {
				foundInBucket[b.Bucket] = true
			}
		}
	}
	for _, b := range buckets {
		assert.True(t, foundInBucket[b.Bucket], "File %s missing from preservation bucket %s", expectedFile.Identifier, b.Bucket)
	}
}

func testPremisEvents(pharosFile, expectedFile *registry.GenericFile) {
	t := ctx.T

	params := url.Values{}
	params.Set("file_identifier", expectedFile.Identifier)
	params.Set("page", "1")
	params.Set("per_page", "50")
	resp := ctx.Context.PharosClient.PremisEventList(params)
	require.Nil(t, resp.Error)
	pharosEvents := resp.PremisEvents()

	// An ingested file should have at least this many events.
	// It can have more if it was ingested more than once.
	require.True(t, len(pharosEvents) >= 5, "Has only %d", len(pharosEvents))

	// Collect events by type...
	urlEvents := findURLAssignmentEvent(pharosEvents)
	fileIdEvents := findIdentifierAssignmentEvent(pharosEvents)
	ingestEvents := findIngestionEvent(pharosEvents)
	replicationEvents := findReplicationEvent(pharosEvents)
	md5Events := findDigestCalculationEvent(pharosEvents, constants.AlgMd5)
	sha1Events := findDigestCalculationEvent(pharosEvents, constants.AlgSha1)
	sha256Events := findDigestCalculationEvent(pharosEvents, constants.AlgSha256)
	sha512Events := findDigestCalculationEvent(pharosEvents, constants.AlgSha512)
	fixityMd5Events := findFixityEvent(pharosEvents, constants.AlgMd5)
	fixitySha256Events := findFixityEvent(pharosEvents, constants.AlgSha256)

	// Most events should appear once for files that were ingested
	// once, and twice for files that were reingested.
	eventCount := 1
	if isReingestFile(expectedFile.Identifier) {
		eventCount = 2
	}
	assert.Equal(t, eventCount, len(ingestEvents), expectedFile.Identifier)
	assert.Equal(t, eventCount, len(md5Events), expectedFile.Identifier)
	assert.Equal(t, eventCount, len(sha1Events), expectedFile.Identifier)
	assert.Equal(t, eventCount, len(sha256Events), expectedFile.Identifier)
	assert.Equal(t, eventCount, len(sha512Events), expectedFile.Identifier)
	assert.Equal(t, eventCount, len(fixityMd5Events), expectedFile.Identifier)
	assert.Equal(t, eventCount, len(fixitySha256Events), expectedFile.Identifier)

	// Standard storage option includes both a primary S3 copy and a
	// replication copy in Glacier. All other storage options currently
	// include only the primary copy with no replication.
	if expectedFile.StorageOption == constants.StorageStandard {
		assert.Equal(t, eventCount, len(replicationEvents), expectedFile.Identifier)
	} else {
		assert.Equal(t, 0, len(replicationEvents), expectedFile.Identifier)
	}

	// URLs and file identifiers should be assigned ONLY ONCE,
	// no matter how many times we ingest a file. Those identifiers
	// are supposed to be persistent.
	assert.Equal(t, 1, len(urlEvents), expectedFile.Identifier)
	assert.Equal(t, 1, len(fileIdEvents), expectedFile.Identifier)
}

func findURLAssignmentEvent(events []*registry.PremisEvent) []*registry.PremisEvent {
	matches := make([]*registry.PremisEvent, 0)
	for _, event := range events {
		if event.OutcomeInformation == "Assigned url identifier" {
			matches = append(matches, event)
		}
	}
	return matches
}

func findIdentifierAssignmentEvent(events []*registry.PremisEvent) []*registry.PremisEvent {
	matches := make([]*registry.PremisEvent, 0)
	for _, event := range events {
		if event.OutcomeInformation == "Assigned bag/filepath identifier" {
			matches = append(matches, event)
		}
	}
	return matches
}

func findIngestionEvent(events []*registry.PremisEvent) []*registry.PremisEvent {
	matches := make([]*registry.PremisEvent, 0)
	for _, event := range events {
		if event.EventType == constants.EventIngestion {
			matches = append(matches, event)
		}
	}
	return matches
}

func findReplicationEvent(events []*registry.PremisEvent) []*registry.PremisEvent {
	matches := make([]*registry.PremisEvent, 0)
	for _, event := range events {
		if event.EventType == constants.EventReplication {
			matches = append(matches, event)
		}
	}
	return matches
}

func findDigestCalculationEvent(events []*registry.PremisEvent, alg string) []*registry.PremisEvent {
	matches := make([]*registry.PremisEvent, 0)
	for _, event := range events {
		if event.EventType == constants.EventDigestCalculation && strings.HasPrefix(event.OutcomeDetail, alg) {
			matches = append(matches, event)
		}
	}
	return matches
}

func findFixityEvent(events []*registry.PremisEvent, alg string) []*registry.PremisEvent {
	matches := make([]*registry.PremisEvent, 0)
	for _, event := range events {
		if event.EventType == constants.EventFixityCheck && strings.HasPrefix(event.OutcomeDetail, alg) {
			matches = append(matches, event)
		}
	}
	return matches
}

func isReingestFile(identifier string) bool {
	return util.StringListContains(ctx.ReingestFiles, identifier)
}

func testWorkItemsAfterIngest() {
	t := ctx.T
	testInst := GetInstitution("test.edu")
	params := url.Values{}
	params.Set("item_action", constants.ActionIngest)
	params.Set("institution_id", strconv.Itoa(testInst.ID))
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

// Pharos doesn't seem to guarantee checksum order, so we have to.
// We really need to fix this on the Pharos end.
func getLatestChecksum(csList []*registry.Checksum, alg string) (checksum *registry.Checksum, err error) {
	if csList == nil || len(csList) == 0 {
		return nil, fmt.Errorf("No checksums in list")
	}
	latest := time.Time{}
	for _, cs := range csList {
		if cs.Algorithm == alg && cs.DateTime.After(latest) {
			checksum = cs
			latest = cs.DateTime
		}
	}
	return checksum, nil
}

// Get the number of checksums having the given algorithm.
// For most bags, there should be one checksum for each algorithm.
// For reingests, there should be two checksums for each algorithm.
func checksumCount(csList []*registry.Checksum, alg string) (count int) {
	for _, cs := range csList {
		if cs.Algorithm == alg {
			count++
		}
	}
	return count
}

func GetInstitution(identifier string) *registry.Institution {
	resp := ctx.Context.PharosClient.InstitutionGet(identifier)
	assert.NotNil(ctx.T, resp)
	require.Nil(ctx.T, resp.Error)
	institution := resp.Institution()
	require.NotNil(ctx.T, institution)
	return institution
}
