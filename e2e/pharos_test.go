// +build e2e

package e2e_test

import (
// "fmt"
// "net/url"
// "testing"

// "github.com/APTrust/preservation-services/constants"
// "github.com/APTrust/preservation-services/models/common"
// "github.com/APTrust/preservation-services/models/registry"
// "github.com/APTrust/preservation-services/util"
// "github.com/stretchr/testify/assert"
// "github.com/stretchr/testify/require"
)

var BagNames = []string{
	"test.edu.apt-001.tar",
	"test.edu.apt-002.tar",
	"test.edu.btr-001.tar",
	"test.edu.btr-002.tar",
}

var ObjectIdentifiers = []string{
	"test.edu/test.edu.apt-001",
	"test.edu/test.edu.apt-002",
	"test.edu/test.edu.btr-001",
	"test.edu/test.edu.btr-002",
}

// // Other test files in e2e_test will call this.
// func GetWorkItems(t *testing.T, context *common.Context) []*registry.WorkItem {
// 	v := url.Values{}
// 	v.Set("name_contains", "test.edu")
// 	v.Set("action", constants.ActionIngest)
// 	resp := context.PharosClient.WorkItemList(v)
// 	require.Nil(t, resp.Error)
// 	return resp.WorkItems()
// }

// // Test that all work items have correct info.
// func TestWorkItems(t *testing.T) {
// 	context := common.NewContext()
// 	items := GetWorkItems(t, context)
// 	for _, bagName := range BagNames {
// 		found := false
// 		for _, item := range items {
// 			if item.Name == bagName {
// 				found = true
// 				testItemObjIdentifier(t, item, bagName)
// 			}
// 		}
// 		assert.True(t, found, bagName)
// 	}
// 	for _, item := range items {
// 		testWorkItem(t, item)
// 	}
// }

// func testWorkItem(t *testing.T, item *registry.WorkItem) {
// 	assert.Equal(t, constants.StageCleanup, item.Stage)
// 	assert.Equal(t, constants.StatusSuccess, item.Status)
// 	assert.Equal(t, "Finished cleanup. Ingest complete.", item.Note)
// }

// func testItemObjIdentifier(t *testing.T, item *registry.WorkItem, bagName string) {
// 	objIdentifier := fmt.Sprintf("test.edu/%s", util.StripFileExtension(bagName))
// 	assert.Equal(t, objIdentifier, item.ObjectIdentifier, bagName)
// }

// // Test that all intellectual object data is present and correct.
// func TestPharosObjects(t *testing.T) {
// 	for _, objIdentifier := range ObjectIdentifiers {
// 		testObject(t, objIdentifier)
// 		testObjectEvents(t, objIdentifier)
// 		testGenericFiles(t, objIdentifier)
// 	}
// }

// func testObject(t *testing.T, objIdentifier string) {

// }

// func testObjectEvents(t *testing.T, objIdentifier string) {

// }

// func testGenericFiles(t *testing.T, objIdentifier string) {

// 	//testFileEvents(t, gfIdentifier)
// 	//testChecksums(t, gfIdentifier)
// }

// func testFileEvents(t *testing.T, gfIdentifier string) {

// }

// func testChecksums(t *testing.T, gfIdentifier string) {

// }
