// +build integration

package ingest_test

import (
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func getFormatIdentifier(t *testing.T) *ingest.FormatIdentifier {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	f := ingest.NewFormatIdentifier(context, testWorkItemId, obj)
	require.NotNil(t, f)
	assert.Equal(t, context, f.Context)
	assert.Equal(t, obj, f.IngestObject)
	assert.Equal(t, testWorkItemId, f.WorkItemId)
	assert.NotNil(t, f.FmtIdentifier)
	return f
}

func TestNewFormatIdentifier(t *testing.T) {
	getFormatIdentifier(t)
}

func TestIdentifyFormat(t *testing.T) {
	// We have to run the staging uploader first to ensure the files
	// we want to identify have been uploaded to our local S3 instance.
	context := common.NewContext()
	stagingUploader := prepareForCopyToStaging(t, context)
	err := stagingUploader.CopyFilesToStaging()
	require.Nil(t, err)

	fi := ingest.NewFormatIdentifier(context,
		stagingUploader.WorkItemId, stagingUploader.IngestObject)

	testGetPresignedURL(t, fi)
	clearFileFormats(t, fi)

	err = fi.IdentifyFormats()
	assert.Nil(t, err)
}

func testGetPresignedURL(t *testing.T, fi *ingest.FormatIdentifier) {
	bucket := fi.Context.Config.StagingBucket
	key := fmt.Sprintf("%d/%s", fi.WorkItemId, constants.EmptyUUID)
	presignedURL, err := fi.GetPresignedURL(bucket, key)
	assert.Nil(t, err)

	bucketAndKey := fmt.Sprintf("%s/%s", bucket, key)
	assert.True(t, strings.Contains(presignedURL.String(), bucketAndKey))
	assert.True(t, strings.Contains(presignedURL.String(), "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential"))
	assert.True(t, strings.Contains(presignedURL.String(), "X-Amz-Signature="))
}

func clearFileFormats(t *testing.T, fi *ingest.FormatIdentifier) {
	clearFn := func(ingestFile *service.IngestFile) error {
		ingestFile.FileFormat = ""
		ingestFile.FormatIdentifiedBy = ""
		ingestFile.FormatIdentifiedAt = time.Time{}
		ingestFile.FormatMatchType = ""
		return nil
	}
	_, err := fi.Context.RedisClient.IngestFilesApply(fi.WorkItemId, clearFn)
	assert.Nil(t, err)
}
