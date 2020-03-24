// +build integration

package ingest_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
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

	f := ingest.NewFormatIdentifier(context,
		stagingUploader.WorkItemId, stagingUploader.IngestObject)

	presignedURL, err := f.GetPresignedURL(context.Config.StagingBucket, constants.EmptyUUID)
	assert.Nil(t, err)
	assert.True(t, strings.Contains(presignedURL.String(), "00000000-0000-0000-0000-000000000000?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential"))
	assert.True(t, strings.Contains(presignedURL.String(), "X-Amz-Signature="))

	err = f.IdentifyFormats()
	assert.Nil(t, err)
}
