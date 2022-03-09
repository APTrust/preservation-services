//go:build integration
// +build integration

package ingest_test

import (
	"path"
	"testing"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fmtWorkItemID = int64(9877)

func getFormatIdentifier(t *testing.T) *ingest.FormatIdentifier {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	f := ingest.NewFormatIdentifier(context, fmtWorkItemID, obj)
	require.NotNil(t, f)
	assert.Equal(t, context, f.Context)
	assert.Equal(t, obj, f.IngestObject)
	assert.Equal(t, fmtWorkItemID, f.WorkItemID)
	assert.NotNil(t, f.Siegfried)
	return f
}

func TestNewFormatIdentifier(t *testing.T) {
	getFormatIdentifier(t)
}

func TestFormatIdentifierRun(t *testing.T) {
	// We have to run the staging uploader first to ensure the files
	// we want to identify have been uploaded to our local S3 instance.
	context := common.NewContext()
	stagingUploader := prepareForCopyToStaging(t, pathToGoodBag, fmtWorkItemID, context)
	_, errors := stagingUploader.Run()
	require.Empty(t, errors)

	fi := ingest.NewFormatIdentifier(context,
		stagingUploader.WorkItemID, stagingUploader.IngestObject)

	clearFileFormats(t, fi)

	numberIdentified, errors := fi.Run()
	assert.Empty(t, errors, errors)
	assert.Equal(t, 16, numberIdentified)

	testFormatMetadata(t, fi)
}

// Note that all files in our test bag are either txt or xml, and some of the
// xml files have no extension.
func testFormatMetadata(t *testing.T, fi *ingest.FormatIdentifier) {
	testFn := func(ingestFile *service.IngestFile) (errors []*service.ProcessingError) {
		if ingestFile.PathInBag == "custom_tags/tracked_file_custom.xml" {
			// Sorry... this one has an invalid and unidentifiable format
			return nil
		}
		if ingestFile.PathInBag == "data/datastream-DC" || ingestFile.PathInBag == "data/datastream-MARC" {
			// Siegfried/PRONOM can't identify these because they have no
			// file extension, and they're missing the opening
			// <xml> tags that signify a proper XML file signature.
			// It does correctly identify other extensionless files
			// that have proper <xml> tags. Why does Fedora export
			// invalid XML?
			assert.Equal(t, "text/plain", ingestFile.FileFormat, ingestFile.PathInBag)
			return nil
		}
		if path.Ext(ingestFile.PathInBag) == ".txt" {
			assert.Equal(t, "text/plain", ingestFile.FileFormat, ingestFile.PathInBag)
		} else {
			assert.True(t, (ingestFile.FileFormat == "text/xml" || ingestFile.FileFormat == "application/xml"), ingestFile.PathInBag)
		}
		assert.Equal(t, constants.FmtIdSiegfried, ingestFile.FormatIdentifiedBy, ingestFile.PathInBag)
		assert.False(t, ingestFile.FormatIdentifiedAt.IsZero(), ingestFile.PathInBag)
		assert.True(t, len(ingestFile.FormatMatchType) > 5, ingestFile.PathInBag)
		return errors
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
	clearFn := func(ingestFile *service.IngestFile) (errors []*service.ProcessingError) {
		ingestFile.FileFormat = ""
		ingestFile.FormatIdentifiedBy = ""
		ingestFile.FormatIdentifiedAt = time.Time{}
		ingestFile.FormatMatchType = ""
		return errors
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
