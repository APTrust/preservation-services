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
	// Create an ingest work item for that object.
	// Save the object and files to Registry.
	// Mark the work item complete.
	// Create a deletion request with requestor.
	// Save deletion request to Registry.
	// Approve the deletion request in registry.
	// Make sure WorkItem was created.
	// Return the IntellectualObjectID

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
