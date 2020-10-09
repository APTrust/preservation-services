// +build e2e

package e2e_test

import (
	//"net/url"
	//"strings"

	//"github.com/APTrust/preservation-services/constants"
	//"github.com/APTrust/preservation-services/e2e"
	"github.com/APTrust/preservation-services/models/registry"
	//"github.com/stretchr/testify/assert"
	//"github.com/stretchr/testify/require"
)

func testDeletionResults() {

}

// func testFileDeletion() {
// 	for _, gfIdentifier := range e2e.FilesToDelete {
// 		// Get GenericFile record from Pharos
// 		// Pass to verifyFileDeletion
// 	}
// }

// func testObjectDeletion() {
// 	for _, objIdentifier := range e2e.ObjectsToDelete {
// 		// Verify object state changed to D
// 		// Verify object deletion event
// 		// Verify all files deleted
// 	}
// }

func verifyFileDeletion(gf *registry.GenericFile) {
	// Verify file state changed to D
	// Verify deletion event in Pharos
	// Verify file deleted from all S3/Glacier buckets
}
