// +build integration

package fixity_test

import (
	"path"
	"testing"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/fixity"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Object identifier is loaded as part of Pharos integration fixtures
var objIdentifier = "institution1.edu/photos"
var fileIdentifier = "institution1.edu/data/test_http_file.txt"

var fileUUID = "8dc5ba50-4a53-4cfc-bb27-6f5e799ace53"
var expectedFixity = "504bb0ab957b554163bb242266dbac895f0fdcd63a1885c573099d901636b0a7"

var setupHasRun = false

func setup(t *testing.T) {
	if !setupHasRun {
		context := common.NewContext()
		storageRecords := copyFileToPreservation(t, context)
		createPharosRecords(t, context, storageRecords)
		setupHasRun = true
	}
}

func copyFileToPreservation(t *testing.T, context *common.Context) (records []*registry.StorageRecord) {
	buckets := context.Config.PreservationBucketsFor(constants.StorageStandard)
	pathToFile := path.Join(testutil.PathToTestData(), "files", "test_http_file.txt")
	for _, bucket := range buckets {
		client := context.S3Clients[bucket.Provider]
		_, err := client.FPutObject(
			bucket.Bucket,
			fileUUID,
			pathToFile,
			minio.PutObjectOptions{},
		)
		require.Nil(t, err)
		records = append(records, &registry.StorageRecord{
			URL: bucket.URLFor(fileUUID),
		})
	}
	return records
}

func createPharosRecords(t *testing.T, context *common.Context, records []*registry.StorageRecord) {
	// Save a GenericFile record
	gf := getGenericFile(t, context)
	resp := context.PharosClient.GenericFileSave(gf)
	require.Nil(t, resp.Error)
	gf = resp.GenericFile() // now has ID

	// Save a sha256 checksum
	now := time.Now().UTC()
	checksum := &registry.Checksum{
		Algorithm:     constants.AlgSha256,
		CreatedAt:     now,
		DateTime:      now,
		Digest:        expectedFixity,
		GenericFileID: gf.ID,
		ID:            0,
		UpdatedAt:     now,
	}
	resp = context.PharosClient.ChecksumSave(checksum, gf.Identifier)
	require.Nil(t, resp.Error)

	// Save the storage records that point to our local
	// Minio integration test server.
	for _, sr := range records {
		sr.GenericFileID = gf.ID
		resp = context.PharosClient.StorageRecordSave(sr, gf.Identifier)
		require.Nil(t, resp.Error)
	}
}

func getGenericFile(t *testing.T, context *common.Context) *registry.GenericFile {
	resp := context.PharosClient.IntellectualObjectGet(objIdentifier)
	require.Nil(t, resp.Error)
	obj := resp.IntellectualObject()
	require.NotNil(t, obj)
	return &registry.GenericFile{
		FileFormat:                   "text/plain",
		FileModified:                 time.Now().UTC(),
		ID:                           0,
		Identifier:                   fileIdentifier,
		InstitutionID:                obj.InstitutionID,
		IntellectualObjectID:         obj.ID,
		IntellectualObjectIdentifier: obj.Identifier,
		Size:                         15,
		State:                        constants.StateActive,
		StorageOption:                constants.StorageStandard,
		UUID:                         fileUUID,
	}
}

func TestNewChecker(t *testing.T) {
	context := common.NewContext()
	checker := fixity.NewChecker(context, fileIdentifier)
	require.NotNil(t, checker)
	assert.Equal(t, context, checker.Context)
	assert.Equal(t, fileIdentifier, checker.Identifier)
}

func TestRun_FixityMatch(t *testing.T) {
	setup(t)
	context := common.NewContext()
	checker := fixity.NewChecker(context, fileIdentifier)
	count, errors := checker.Run()
	assert.Equal(t, 1, count)
	assert.Empty(t, errors)
}

func TestSupportingMethods(t *testing.T) {
	setup(t)
	context := common.NewContext()
	checker := fixity.NewChecker(context, fileIdentifier)

	// Need to get GenericFile with StorageRecords
	gf := context.PharosClient.GenericFileGet(fileIdentifier).GenericFile()
	actualFixity, url, err := checker.CalculateFixity(gf)
	assert.Equal(t, expectedFixity, actualFixity)
	assert.Equal(t, "https://s3.us-east-1.localhost:9899/preservation-va/8dc5ba50-4a53-4cfc-bb27-6f5e799ace53", url)
	assert.Nil(t, err)

	matched, err := checker.RecordFixityEvent(gf, url, expectedFixity, actualFixity)
	assert.True(t, matched)
	require.Nil(t, err)

	matched, err = checker.RecordFixityEvent(gf, url, expectedFixity, "this-will-not-match")
	assert.False(t, matched)
	require.Nil(t, err)
}
