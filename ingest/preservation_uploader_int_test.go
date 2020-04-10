// +build integration

package ingest_test

import (
	"testing"

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
	uploader := prepareForPreservationUpload(t, pathToGoodBag, context)
	filesUploaded, errors := uploader.UploadAll()
	require.Empty(t, errors, errors)

	// Bag has 16 files, all of which should have been
	// processed, though not all will have been uplaoded.
	assert.Equal(t, 16, filesUploaded)

	testStorageRecords(t, uploader)
	testFilesAreInRightBuckets(t, uploader)

	fileIdentifier := uploader.IngestObject.FileIdentifier("aptrust-info.txt")
	testCopyToAWSPreservation(t, uploader, fileIdentifier)
	testCopyToExternalPreservation(t, uploader, fileIdentifier)
}

// This function tests that each file was copied to both
// preservation buckets (VA and OR for Standard storage),
// and that they have timestamps indicating when the
// copy occurred.
func testStorageRecords(t *testing.T, uploader *ingest.PreservationUploader) {
	config := uploader.Context.Config
	uploadCount := 0
	testFn := func(ingestFile *service.IngestFile) (errors []*service.ProcessingError) {
		if ingestFile.HasPreservableName() {
			assert.Equal(t, 2, len(ingestFile.StorageRecords))
			for _, record := range ingestFile.StorageRecords {
				//fmt.Println(record)
				uploadCount++
				assert.True(t, record.Bucket == config.BucketStandardVA || record.Bucket == config.BucketStandardOR)
				assert.False(t, record.StoredAt.IsZero())
			}
		} else {
			// If HasPreservableName() is false, file should not
			// have been copied to preservation,
			assert.Equal(t, 0, len(ingestFile.StorageRecords))
		}
		return errors
	}
	options := service.IngestFileApplyOptions{
		MaxErrors:   1,
		MaxRetries:  1,
		RetryMs:     0,
		SaveChanges: false,
		WorkItemID:  uploader.WorkItemID,
	}
	_, errors := uploader.Context.RedisClient.IngestFilesApply(testFn, options)
	assert.Empty(t, errors, errors)

	// We should have preserved 11 of this bag's 16 files.
	// bagit.txt, manifests and tag manifests are not preserved.
	// 11 files times 2 copies each equals 22 files.
	assert.Equal(t, 22, uploadCount)
}

func testFilesAreInRightBuckets(t *testing.T, uploader *ingest.PreservationUploader) {
	buckets := []string{
		uploader.Context.Config.BucketStandardVA,
		uploader.Context.Config.BucketStandardOR,
	}
	testFn := func(ingestFile *service.IngestFile) (errors []*service.ProcessingError) {
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
		return errors
	}
	options := service.IngestFileApplyOptions{
		MaxErrors:   1,
		MaxRetries:  1,
		RetryMs:     0,
		SaveChanges: false,
		WorkItemID:  uploader.WorkItemID,
	}
	_, errors := uploader.Context.RedisClient.IngestFilesApply(testFn, options)
	assert.Empty(t, errors, errors)
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

func testCopyToExternalPreservation(t *testing.T, uploader *ingest.PreservationUploader, fileIdentifier string) {
	ingestFile, err := uploader.Context.RedisClient.IngestFileGet(
		uploader.WorkItemID,
		fileIdentifier,
	)
	require.Nil(t, err)
	require.NotNil(t, ingestFile)

	uploadTarget := uploader.Context.Config.UploadTargetsFor(constants.StorageWasabiOR)[0]

	err = uploader.CopyToExternalPreservation(ingestFile, uploadTarget)
	require.Nil(t, err)

	stats, err := uploader.Context.S3StatObject(
		constants.StorageProviderWasabi,
		uploadTarget.Bucket,
		ingestFile.UUID,
	)
	require.Nil(t, err)
	require.EqualValues(t, ingestFile.Size, stats.Size)

}
