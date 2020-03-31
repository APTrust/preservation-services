// +build integration

package ingest_test

import (
	"fmt"
	//"path"
	//"strings"
	"testing"
	//"time"

	"github.com/APTrust/preservation-services/constants"
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

	testStorageRecords(t, uploader)
	testFilesAreInRightBuckets(t, uploader)

	fileIdentifier := uploader.IngestObject.FileIdentifier("aptrust-info.txt")
	testCopyToAWSPreservation(t, uploader, fileIdentifier)
}

// This function tests that each file was copied to both
// preservation buckets (VA and OR for Standard storage),
// and that they have timestamps indicating when the
// copy occurred.
func testStorageRecords(t *testing.T, uploader *ingest.PreservationUploader) {
	config := uploader.Context.Config
	testFn := func(ingestFile *service.IngestFile) error {
		if ingestFile.HasPreservableName() {
			assert.Equal(t, 2, len(ingestFile.StorageRecords))
			fmt.Println(" >>> YES >>> ", ingestFile.PathInBag)
			for _, record := range ingestFile.StorageRecords {
				//fmt.Println(record)
				assert.True(t, record.Bucket == config.BucketStandardVA || record.Bucket == config.BucketStandardOR)
				assert.False(t, record.StoredAt.IsZero())
			}
		} else {
			// If HasPreservableName() is false, file should not
			// have been copied to preservation,
			fmt.Println(" >>> NO >>> ", ingestFile.PathInBag)
			assert.Equal(t, 0, len(ingestFile.StorageRecords))
		}
		return nil
	}
	uploader.Context.RedisClient.IngestFilesApply(uploader.WorkItemID, testFn)
}

func testFilesAreInRightBuckets(t *testing.T, uploader *ingest.PreservationUploader) {
	buckets := []string{
		uploader.Context.Config.BucketStandardVA,
		uploader.Context.Config.BucketStandardOR,
	}
	testFn := func(ingestFile *service.IngestFile) error {
		if ingestFile.HasPreservableName() {
			for _, bucket := range buckets {
				stats, err := uploader.Context.S3StatObject(
					constants.StorageProviderAWS,
					bucket,
					ingestFile.UUID,
				)
				require.Nil(t, err)
				assert.EqualValues(t, ingestFile.Size, stats.Size)
			}
		} else {
			// If HasPreservableName() is false, file should not
			// have been copied to preservation,
			assert.Equal(t, 0, len(ingestFile.StorageRecords))
		}
		return nil
	}
	uploader.Context.RedisClient.IngestFilesApply(uploader.WorkItemID, testFn)

}

func testCopyToAWSPreservation(t *testing.T, uploader *ingest.PreservationUploader, fileIdentifier string) {
	ingestFile, err := uploader.Context.RedisClient.IngestFileGet(
		uploader.WorkItemID,
		fileIdentifier,
	)
	require.Nil(t, err)
	require.NotNil(t, ingestFile)

	uploadTarget := uploader.Context.Config.UploadTargetsFor(constants.StorageGlacierOH)[0]

	err = uploader.CopyToAWSPreservation(ingestFile, uploadTarget)
	require.Nil(t, err)

	stats, err := uploader.Context.S3StatObject(
		constants.StorageProviderAWS,
		uploadTarget.Bucket,
		ingestFile.UUID,
	)
	require.Nil(t, err)
	require.EqualValues(t, ingestFile.Size, stats.Size)
}

func testCopyToExternalPreservation(t *testing.T, uploader *ingest.PreservationUploader) {

}
