// +build integration

package ingest_test

import (
	//"fmt"
	//"path"
	//"strings"
	"testing"
	//"time"

	//"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPreservationUploader(t *testing.T) {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	uploader := ingest.NewPreservationUploader(context, testWorkItemId, obj)
	require.NotNil(t, uploader)
	assert.Equal(t, context, uploader.Context)
	assert.Equal(t, obj, uploader.IngestObject)
	assert.Equal(t, testWorkItemId, uploader.WorkItemID)
}

func TestPreservationUploadAll(t *testing.T) {
	context := common.NewContext()
	uploader := prepareForPreservationUpload(t, context)
	_, err := uploader.UploadAll()
	require.Nil(t, err)

	// Should be 10 files each copied to 2 locations, for a
	// total of 20 files uploaded.
	// assert.Equal(t, 20, filesUploaded)

	testFn := func(ingestFile *service.IngestFile) error {
		if ingestFile.HasPreservableName() {
			assert.Equal(t, 2, len(ingestFile.StorageRecords))
			//fmt.Println(ingestFile.PathInBag)
			for _, record := range ingestFile.StorageRecords {
				//fmt.Println(record)
				assert.True(t, record.Bucket == context.Config.BucketStandardVA || record.Bucket == context.Config.BucketStandardOR)
				assert.False(t, record.StoredAt.IsZero())
			}
		}
		return nil
	}

	context.RedisClient.IngestFilesApply(uploader.WorkItemID, testFn)
}

func testCopyToAWSPreservation(t *testing.T) {

}

func testCopyToExternalPreservation(t *testing.T) {

}
