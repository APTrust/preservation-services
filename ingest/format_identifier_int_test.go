// +build integration

package ingest_test

import (
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getFormatIdentifier(t *testing.T) *ingest.FormatIdentifier {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	f := ingest.NewFormatIdentifier(context, testWorkItemId, obj)
	require.NotNil(t, f)
	assert.Equal(t, context, f.Context)
	assert.Equal(t, obj, f.IngestObject)
	assert.Equal(t, testWorkItemId, f.WorkItemID)
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
		stagingUploader.WorkItemID, stagingUploader.IngestObject)

	testGetPresignedURL(t, fi)
	clearFileFormats(t, fi)

	numberIdentified, errors := fi.IdentifyFormats()
	assert.Empty(t, errors, errors)
	assert.Equal(t, 16, numberIdentified)

	testFormatMetadata(t, fi)
}

func testGetPresignedURL(t *testing.T, fi *ingest.FormatIdentifier) {
	bucket := fi.Context.Config.StagingBucket
	ingestFile := &service.IngestFile{
		UUID: constants.EmptyUUID,
	}
	key := fi.S3KeyFor(ingestFile)
	presignedURL, err := fi.GetPresignedURL(bucket, key)
	assert.Nil(t, err)

	bucketAndKey := fmt.Sprintf("%s/%s", bucket, key)
	assert.True(t, strings.Contains(presignedURL.String(), bucketAndKey))
	assert.True(t, strings.Contains(presignedURL.String(), "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential"))
	assert.True(t, strings.Contains(presignedURL.String(), "X-Amz-Signature="))
}

// Note that all files in our test bag are either txt or xml, and some of the
// xml files have no extension.
func testFormatMetadata(t *testing.T, fi *ingest.FormatIdentifier) {
	testFn := func(ingestFile *service.IngestFile) error {
		if ingestFile.PathInBag == "data/datastream-DC" || ingestFile.PathInBag == "data/datastream-MARC" {
			// Fido can't identify these because they have no
			// file extension, and they're missing the opening
			// <xml> tags that signify a proper XML file signature.
			// It does correctly identify other extensionless files
			// that have proper <xml> tags. Why does Fedora export
			// invalid XML?
			return nil
		}
		if path.Ext(ingestFile.PathInBag) == ".txt" {
			assert.Equal(t, "text/plain", ingestFile.FileFormat, ingestFile.PathInBag)
		} else {
			assert.True(t, (ingestFile.FileFormat == "text/xml" || ingestFile.FileFormat == "application/xml"), ingestFile.PathInBag)
		}
		assert.Equal(t, constants.FmtIdFido, ingestFile.FormatIdentifiedBy, ingestFile.PathInBag)
		assert.False(t, ingestFile.FormatIdentifiedAt.IsZero(), ingestFile.PathInBag)
		assert.True(t, (ingestFile.FormatMatchType == constants.MatchTypeSignature || ingestFile.FormatMatchType == constants.MatchTypeExtension), ingestFile.PathInBag)
		return nil
	}
	options := service.IngestFileApplyOptions{
		MaxErrors:   1,
		MaxRetries:  1,
		RetryMs:     0,
		SaveChanges: false,
		WorkItemID:  fi.WorkItemID,
	}
	_, errors := fi.Context.RedisClient.IngestFilesApply(testFn, options)
	assert.Empty(t, errors, errors)
}

func clearFileFormats(t *testing.T, fi *ingest.FormatIdentifier) {
	clearFn := func(ingestFile *service.IngestFile) error {
		ingestFile.FileFormat = ""
		ingestFile.FormatIdentifiedBy = ""
		ingestFile.FormatIdentifiedAt = time.Time{}
		ingestFile.FormatMatchType = ""
		return nil
	}
	options := service.IngestFileApplyOptions{
		MaxErrors:   1,
		MaxRetries:  1,
		RetryMs:     0,
		SaveChanges: true,
		WorkItemID:  fi.WorkItemID,
	}
	_, errors := fi.Context.RedisClient.IngestFilesApply(clearFn, options)
	assert.Empty(t, errors, errors)
}
