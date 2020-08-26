// +build integration

package restoration_test

import (
	"io/ioutil"
	"path"
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/restoration"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var bagRestorerSetupCompleted = false

const workItemID = 334455

type RestorationItem struct {
	WorkItemID    int
	ObjIdentifier string
	BagItProfile  string
}

var itemsToRestore = []RestorationItem{
	RestorationItem{
		WorkItemID:    87777,
		ObjIdentifier: "test.edu/apt-test-restore",
		BagItProfile:  constants.BagItProfileDefault,
	},
	RestorationItem{
		WorkItemID:    87999,
		ObjIdentifier: "test.edu/btr-512-test-restore",
		BagItProfile:  constants.BagItProfileBTR,
	},
}

// setup ensures the files we want to restore are in the local Minio
// preservation buckets. All other info pertaining to these files/bags
// is loaded from fixture data into Pharos by the test script in
// scripts/test.rb
func setup(t *testing.T, context *common.Context) {
	if bagRestorerSetupCompleted {
		return
	}

	s3Client := context.S3Clients[constants.StorageProviderAWS]

	// Our test files should be in these two preservation buckets,
	// according to the Pharos fixture data.
	preservationBuckets := []string{
		context.Config.BucketStandardVA,
		context.Config.BucketStandardOR,
	}

	// Copy the files from int_test_bags/restoration/files to the
	// local Minio preservation buckets.
	dir := path.Join(testutil.PathToTestData(), "int_test_bags", "restoration", "files")
	files, err := ioutil.ReadDir(dir)
	require.Nil(t, err)
	for _, file := range files {
		fullpath := path.Join(dir, file.Name())
		for _, bucket := range preservationBuckets {
			_, err := s3Client.FPutObject(
				bucket,
				file.Name(),
				fullpath,
				minio.PutObjectOptions{})
			require.Nil(t, err)
		}
	}
	bagRestorerSetupCompleted = true
}

func getRestorationObject(objIdentifier string) *service.RestorationObject {
	return &service.RestorationObject{
		Identifier:             objIdentifier,
		BagItProfileIdentifier: constants.DefaultProfileIdentifier,
		RestorationSource:      constants.RestorationSourceS3,
		RestorationTarget:      "aptrust.restore.test.test.edu",
		RestorationType:        constants.RestorationTypeObject,
	}
}

func TestNewBagRestorer(t *testing.T) {
	item := itemsToRestore[0]
	restorer := restoration.NewBagRestorer(
		common.NewContext(),
		item.WorkItemID,
		getRestorationObject(item.ObjIdentifier))
	require.NotNil(t, restorer)
	require.NotNil(t, restorer.Context)
	assert.Equal(t, item.WorkItemID, restorer.WorkItemID)
	assert.Equal(t, item.ObjIdentifier, restorer.RestorationObject.Identifier)
}

func TestBagRestorer_Run(t *testing.T) {
	context := common.NewContext()
	setup(t, context)
	for _, item := range itemsToRestore {
		restObj := getRestorationObject(item.ObjIdentifier)
		restorer := restoration.NewBagRestorer(context, item.WorkItemID, restObj)
		fileCount, errors := restorer.Run()
		assert.True(t, fileCount >= 3)
		assert.Empty(t, errors)
		testRestoredBag(t, context, item)
	}
}

func getIngestObject(objIdentifier string) *service.IngestObject {
	return &service.IngestObject{
		Institution: "test.edu",
		S3Bucket:    "aptrust.restore.test.test.edu",
		S3Key:       objIdentifier + ".tar",
	}
}

func testRestoredBag(t *testing.T, context *common.Context, item RestorationItem) {
	ingestObj := getIngestObject(item.ObjIdentifier)
	m := ingest.NewMetadataGatherer(context, item.WorkItemID, ingestObj)
	fileCount, errors := m.Run()
	assert.Empty(t, errors)

	// fileCount is count of all files in bag, including manifests.
	// APTrust bag has one extra: aptrust-info.txt.
	if item.ObjIdentifier == "test.edu/apt-test-restore" {
		assert.Equal(t, 9, fileCount)
	} else {
		assert.Equal(t, 8, fileCount)
	}

	// Validate the bag
	v := ingest.NewMetadataValidator(context, item.WorkItemID, ingestObj)
	fileCount, errors = v.Run()
	assert.Empty(t, errors)

}

// func TestBagRestorer_GetManifestPath(t *testing.T) {

// }

// func TestBagRestorer_DeleteStaleManifests(t *testing.T) {

// }

// func TestBagRestorer_BestRestorationSource(t *testing.T) {

// }
