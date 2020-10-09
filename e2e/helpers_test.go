// +build e2e

package e2e_test

import (
	"fmt"
	"net/url"
	"strings"
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

// This file contains helpers to do some setup and
// housekeeping but don't perform any actual tests.

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
	ctx.TestInstitution = getInstitution("test.edu")
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

func waitForRestorationCompletion() {
	for {
		if restorationsComplete() {
			break
		}
		time.Sleep(10 * time.Second)
	}
}

func waitForFixityCompletion() {
	for {
		if fixitiesComplete() {
			break
		}
		time.Sleep(10 * time.Second)
	}
}

func queueFixityItems() {
	for _, testFile := range e2e.FilesForFixityCheck {
		err := ctx.Context.NSQClient.EnqueueString(constants.TopicFixity, testFile.Identifier)
		require.Nil(ctx.T, err, testFile.Identifier)
	}
}

// This returns the number of bags expected to be ingested
// or reingested. The reingest count includes all ingests
// plus reingests.
func testBagCount(ingestType string) int64 {
	count := int64(0)
	for _, tb := range e2e.TestBags {
		if (ingestType == "ingest" && !tb.IsUpdate) || (ingestType == "reingest" && tb.IsUpdate) {
			count++
		}
	}
	return count
}

// Returns the number of expected ingests, based on test bags defined
// in TestBags (in expected.go)
func expectedIngestCount() int64 {
	return testBagCount("ingest")
}

// Returns the number of expected reingests, based on test bags defined
// in TestBags (in expected.go)
func expectedReingestCount() int64 {
	return testBagCount("reingest")
}

// Returns true if the initial version of our test bags have
// been ingested.
func initialIngestsComplete() bool {
	return allItemsInTopic(constants.TopicE2EIngest, expectedIngestCount())
}

// Returns true if the updated versions of our test bags have
// been ingested.
func reingestsComplete() bool {
	return allItemsInTopic(constants.TopicE2EReingest, expectedReingestCount())
}

func restorationsComplete() bool {
	count := int64(len(e2e.FilesToRestore) + len(e2e.BagsToRestore))
	return allItemsInTopic(constants.TopicE2ERestore, count)
}

func fixitiesComplete() bool {
	return allItemsInTopic(constants.TopicE2EFixity, int64(len(e2e.FilesForFixityCheck)))
}

// This queries NSQ to find the number of items that have been pushed
// into a topic.
func allItemsInTopic(topicName string, desiredCount int64) bool {
	require.True(ctx.T, desiredCount > 0)
	stats, err := ctx.Context.NSQClient.GetStats()
	require.Nil(ctx.T, err)
	allComplete := false
	topicStats := stats.GetTopic(topicName)
	if topicStats == nil {
		// Topic won't exist until the first ingest/reingest is complete.
		ctx.Context.Logger.Infof("Topic %s hasn't been created yet", topicName)
	} else {
		ctx.Context.Logger.Infof("Topic %s has depth %d. Want %d", topicName, topicStats.Depth, desiredCount)
		allComplete = (topicStats.Depth == desiredCount)
	}
	return allComplete
}

func objIdentFromFileIdent(gfIdentifier string) string {
	parts := strings.Split(gfIdentifier, "/")
	if len(parts) < 3 {
		return "INVALID FILE IDENTIFIER"
	}
	return strings.Join(parts[0:2], "/")
}

// Returns an institution record from Pharos. Our "test.edu" institution
// will have a different ID each time we test, so we have to look it up.
func getInstitution(identifier string) *registry.Institution {
	resp := ctx.Context.PharosClient.InstitutionGet(identifier)
	assert.NotNil(ctx.T, resp)
	require.Nil(ctx.T, resp.Error)
	institution := resp.Institution()
	require.NotNil(ctx.T, institution)
	return institution
}

func createRestorationWorkItems() (err error) {
	// create 4 file restorations
	for _, testFile := range e2e.FilesToRestore {
		objIdentifier := objIdentFromFileIdent(testFile.Identifier)
		err = createRestorationWorkItem(objIdentifier, testFile.Identifier)
		if err != nil {
			return err
		}
	}

	// create 2 APTrust and 2 BTR bag restorations
	// one original and one updated bag from APTrust, BTR
	for _, objIdentifier := range e2e.BagsToRestore {
		err = createRestorationWorkItem(objIdentifier, "")
		if err != nil {
			return err
		}
	}
	return nil
}

func createRestorationWorkItem(objIdentifier, gfIdentifier string) error {
	ingestItem, err := getLastIngestRecord(objIdentifier)
	if err != nil {
		return err
	}
	utcNow := time.Now().UTC()
	restorationItem := &registry.WorkItem{
		Action:                constants.ActionRestore,
		BagDate:               ingestItem.BagDate,
		Bucket:                ingestItem.Bucket,
		CreatedAt:             utcNow,
		Date:                  ingestItem.Date,
		ETag:                  ingestItem.ETag,
		GenericFileIdentifier: gfIdentifier,
		InstitutionID:         ingestItem.InstitutionID,
		Name:                  ingestItem.Name,
		Note:                  "Restoration requested",
		ObjectIdentifier:      objIdentifier,
		Outcome:               "Restoration requested",
		Retry:                 true,
		Size:                  ingestItem.Size,
		Stage:                 constants.StageRequested,
		Status:                constants.StatusPending,
		User:                  "e2e@aptrust.org",
	}
	resp := ctx.Context.PharosClient.WorkItemSave(restorationItem)
	return resp.Error
}

func getLastIngestRecord(objIdentifier string) (*registry.WorkItem, error) {
	params := url.Values{}
	params.Set("object_identifier", objIdentifier)
	params.Set("item_action", constants.ActionIngest)
	params.Set("stage", constants.StageCleanup)
	params.Set("status", constants.StatusSuccess)
	params.Set("sort", "date desc")
	params.Set("page", "1")
	params.Set("per_page", "1")
	resp := ctx.Context.PharosClient.WorkItemList(params)
	if resp.Error != nil {
		return nil, resp.Error
	}
	items := resp.WorkItems()
	if len(items) < 1 {
		return nil, fmt.Errorf("No ingest WorkItems for %s", objIdentifier)
	}
	return items[0], nil
}

func getRestoreWorkItems(objIdentifier, gfIdentifier string) []*registry.WorkItem {
	params := url.Values{}
	params.Set("object_identifier", objIdentifier)
	params.Set("file_identifier", gfIdentifier)
	params.Set("item_action", constants.ActionRestore)
	resp := ctx.Context.PharosClient.WorkItemList(params)
	require.Nil(ctx.T, resp.Error)
	return resp.WorkItems()
}

func createDeletionWorkItems() {
	for _, gfIdentifier := range e2e.FilesToDelete {
		objIdentifier := objIdentFromFileIdent(gfIdentifier)
		err := createDeletionWorkItem(objIdentifier, gfIdentifier)
		assert.Nil(ctx.T, err, gfIdentifier)
	}
	for _, objIdentifier := range e2e.ObjectsToDelete {
		err := createDeletionWorkItem(objIdentifier, "")
		assert.Nil(ctx.T, err, objIdentifier)
	}
}

func createDeletionWorkItem(objIdentifier, gfIdentifier string) error {
	ingestItem, err := getLastIngestRecord(objIdentifier)
	if err != nil {
		return err
	}
	utcNow := time.Now().UTC()
	deletionItem := &registry.WorkItem{
		Action:                constants.ActionDelete,
		BagDate:               ingestItem.BagDate,
		Bucket:                ingestItem.Bucket,
		CreatedAt:             utcNow,
		Date:                  ingestItem.Date,
		ETag:                  ingestItem.ETag,
		GenericFileIdentifier: gfIdentifier,
		InstApprover:          "approver@example.com",
		InstitutionID:         ingestItem.InstitutionID,
		Name:                  ingestItem.Name,
		Note:                  "Deletion requested",
		ObjectIdentifier:      objIdentifier,
		Outcome:               "Deletion requested",
		Retry:                 true,
		Size:                  ingestItem.Size,
		Stage:                 constants.StageRequested,
		Status:                constants.StatusPending,
		User:                  "e2e@aptrust.org",
	}
	resp := ctx.Context.PharosClient.WorkItemSave(deletionItem)
	return resp.Error
}
