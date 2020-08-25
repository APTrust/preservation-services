// +build integration

package restoration_test

import (
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/restoration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO: Mock Pharos and S3 here? Or do full integration test?
// The second option will require ingesting a bag.
//  - Copy bag to receiving bucket
//  - Create WorkItem (using bucket reader "run once"?)
//  - Poll for completion of ingest
//  - Run tests

const workItemID = 9999

func getRestorationObject() *service.RestorationObject {
	return &service.RestorationObject{
		Identifier:             "test.edu/apt-bag-1",
		BagItProfileIdentifier: constants.DefaultProfileIdentifier,
		FileSize:               6500000,
		RestorationSource:      constants.RestorationSourceS3,
		RestorationTarget:      "aptrust.restore.test.test.edu",
		RestorationType:        constants.RestorationTypeObject,
	}
}

func getRestorer() *restoration.BagRestorer {
	return restoration.NewBagRestorer(common.NewContext(), workItemID, getRestorationObject())
}

func TestNewBagRestorer(t *testing.T) {
	r := getRestorer()
	require.NotNil(t, r)
	require.NotNil(t, r.Context)
	assert.Equal(t, workItemID, r.WorkItemID)
	assert.Equal(t, "test.edu/apt-bag-1", r.RestorationObject.Identifier)
}

func TestBagRestorer_RecordDigests(t *testing.T) {

}

func TestBagRestorer_AppendDigestToManifest(t *testing.T) {

}

func TestBagRestorer_GetManifestPath(t *testing.T) {

}

func TestBagRestorer_DeleteStaleManifests(t *testing.T) {

}

func TestBagRestorer_BestRestorationSource(t *testing.T) {

}

func TestBagRestorer_GetBatchOfFiles(t *testing.T) {

}

func TestBagRestorer_GetTarHeader(t *testing.T) {

}

func TestBagRestorer_AddBagItFile(t *testing.T) {

}

func TestBagRestorer_AddManifests(t *testing.T) {

}

func TestBagRestorer_AddToTarFile(t *testing.T) {

}

func TestBagRestorer_Run(t *testing.T) {

}
