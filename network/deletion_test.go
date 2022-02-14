package network_test

// This file contains helpers to set up object and file deletion tests,
// and to test post-conditions.

import (
	//"fmt"
	"net/http"
	//"net/url"
	"testing"
	//"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/network"
	"github.com/APTrust/preservation-services/util/logger"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupObjectDeleteTest(t *testing.T, client *network.RegistryClient) {
	// Create an object with four files.
	obj := testutil.GetIntellectualObject()
	obj.Identifier = "test.edu/ObjForDeletion01"
	obj.BagName = "ObjForDeletion01"
	obj.FileCount = 4
	resp := client.IntellectualObjectSave(obj)
	require.Nil(t, resp.Error)
	obj = resp.IntellectualObject()
	require.NotNil(t, obj)
	require.NotEmpty(t, obj.ID)

	// Add some files to the object, complete with checksums,
	// events, and storage records.
	files := make([]*registry.GenericFile, 4)
	for i := 0; i < 4; i++ {
		gf := testutil.GetGenericFileForObj(obj, i, true, true)
		gf.StorageRecords = append(
			gf.StorageRecords,
			&registry.StorageRecord{
				URL: fmt.Sprintf("https://example.com/storage/record/%d", gf.UUID),
			})
		files[i] = gf
	}
	resp := client.GenericFileCreateBatch(files)

	// Create an ingest work item for that object.
	now := time.Now().UTC()
	yesterday := now.Add(-24 * time.Hour)
	item := &registry.WorkItem{
		Action:        constants.ActionIngest,
		BagDate:       yesterday,
		Bucket:        "buckeypoo",
		Date:          now,
		ETag:          obj.ETag,
		InstitutionID: obj.InstitutionID,
		Name:          obj.BagName,
		Note:          "Item was successfully ingested",
		Outcome:       constants.OutcomeSuccess,
		Retry:         true,
		Size:          int64(9999000),
		Stage:         constants.StageCleanup,
		Status:        constants.StatusSuccess,
		User:          "test@test.edu",
	}
	resp = client.WorkItemSave(item)
	require.Nil(t, resp.Error)
	item = resp.WorkItem()
	require.NotNil(t, item)
	requier.NotEmpty(t, item.ID)

	// Create a deletion request with requester.
	// This will require a special endpoint in Registry, and a special
	// method on the Registry client.

	// Approve the deletion request in registry.
	// This creates the deletion work item, most of whose attributes
	// are cloned from the ingest work item we created above.

	// Make sure WorkItem was created.

	// Return IntellectualObjectID to caller, so it knows which item
	// it can delete.

	// Initial deletion attempt will fail.
	// Caller will have to mark all files with State="D"
	// and then try again.
}

func setupFileDeleteTest(t *testing.T, client *network.RegistryClient) {
	// Run setupObjectDeleteTest above.
	// Return one GenericFileID from that.
}

func testObjectPostDeletionConditions(t *testing.T, client *network.RegistryClient, objID int64) {
	// Make sure object state = "D"
	// Make sure object has deletion premis event
	// Make sure file post deletion tests passes for all files
}

func testFilePostDeletionConditions(t *testing.T, client *network.RegistryClient, gfID int64) {
	// Make sure file state is "D"
	// Make sure file has no remaining Storage reacords
	// Make sure file has deletion premis event
}

func testDeletionWorkItemComplete(t *testing.T, client *network.RegistryClient, workItemID int64) {
	// Make sure deletion WorkItem is marked as complete
	// and all fields are set as expected.
}

func testDeletionRequestComplete(t *testing.T, client *network.RegistryClient, workItemID int64) {
	// Make sure the deletion request is marked as complete
	// and all fields are set as expected.
}
