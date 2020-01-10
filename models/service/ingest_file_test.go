package service_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewIngestFile(t *testing.T) {
	f := service.NewIngestFile(testutil.ObjIdentifier, "data/image.jpg")
	assert.NotNil(t, f.Checksums)
	assert.EqualValues(t, 0, f.Id)
	assert.Equal(t, testutil.ObjIdentifier, f.ObjectIdentifier)
	assert.True(t, f.NeedsSave)
	assert.Equal(t, "data/image.jpg", f.PathInBag)
	assert.Equal(t, "Standard", f.StorageOption)
	assert.NotNil(t, f.StorageRecords)
}

func TestFileFromJson(t *testing.T) {
	expectedFile := testutil.GetIngestFile(true, true)
	f, err := service.IngestFileFromJson(testutil.IngestFileJson)
	assert.Nil(t, err)
	assert.Equal(t, expectedFile.Checksums, f.Checksums)
	assert.Equal(t, expectedFile.ObjectIdentifier, f.ObjectIdentifier)
	assert.Equal(t, expectedFile.PathInBag, f.PathInBag)
}

func TestFileToJson(t *testing.T) {
	f := testutil.GetIngestFile(true, true)
	data, err := f.ToJson()
	assert.Nil(t, err)
	assert.Equal(t, testutil.IngestFileJson, data)
}

func TestIdentifier(t *testing.T) {
	f := service.NewIngestFile(testutil.ObjIdentifier, "data/image.jpg")
	assert.Equal(t, "test.edu/test-bag/data/image.jpg", f.Identifier())
}

func TestFileType(t *testing.T) {
	f := testutil.GetIngestFile(false, false)
	assert.Equal(t, constants.FileTypePayload, f.FileType())

	f.PathInBag = "manifest-md5.txt"
	assert.Equal(t, constants.FileTypeManifest, f.FileType())

	f.PathInBag = "tagmanifest-sha256.txt"
	assert.Equal(t, constants.FileTypeTagManifest, f.FileType())

	f.PathInBag = "bag-info.txt"
	assert.Equal(t, constants.FileTypeTag, f.FileType())

	f.PathInBag = "custom-tags/somefile.txt"
	assert.Equal(t, constants.FileTypeTag, f.FileType())

	f.PathInBag = "fetch.txt"
	assert.Equal(t, constants.FileTypeFetchTxt, f.FileType())
}

func TestIsParsableTagFile(t *testing.T) {
	f := testutil.GetIngestFile(false, false)
	assert.False(t, f.IsParsableTagFile())

	files := []string{
		"aptrust-info.txt",
		"bag-info.txt",
		"bagit.txt",
	}
	for _, fname := range files {
		f.PathInBag = fname
		assert.True(t, f.IsParsableTagFile())
	}
}

func TestSetChecksum(t *testing.T) {
	f := testutil.GetIngestFile(false, false)
	firstMd5 := testutil.GetIngestChecksum(constants.AlgMd5, constants.SourceIngest)
	secondMd5 := testutil.GetIngestChecksum(constants.AlgMd5, constants.SourceRegistry)

	f.SetChecksum(firstMd5)
	assert.Equal(t, 1, len(f.Checksums))
	assert.Equal(t, "md5:ingest", f.Checksums[0].Digest)

	f.SetChecksum(secondMd5)
	assert.Equal(t, 2, len(f.Checksums))
	assert.Equal(t, "md5:registry", f.Checksums[1].Digest)

	// Reseting the a checksum should update, not append
	firstMd5.Digest = "first-updated"
	f.SetChecksum(firstMd5)
	assert.Equal(t, 2, len(f.Checksums))
	assert.Equal(t, "first-updated", f.Checksums[0].Digest)
}

func TestGetChecksum(t *testing.T) {
	f := testutil.GetIngestFile(true, false)

	ingestMd5 := f.GetChecksum(constants.SourceIngest, constants.AlgMd5)
	assert.Equal(t, constants.SourceIngest, ingestMd5.Source)
	assert.Equal(t, constants.AlgMd5, ingestMd5.Algorithm)
	assert.Equal(t, "md5:ingest", ingestMd5.Digest)

	registryMd5 := f.GetChecksum(constants.SourceRegistry, constants.AlgMd5)
	assert.Equal(t, constants.SourceRegistry, registryMd5.Source)
	assert.Equal(t, constants.AlgMd5, registryMd5.Algorithm)
	assert.Equal(t, "md5:registry", registryMd5.Digest)

	nilChecksum := f.GetChecksum(constants.SourceManifest, constants.AlgSha256)
	assert.Nil(t, nilChecksum)
}

func TestSetStorageRecord(t *testing.T) {
	f := testutil.GetIngestFile(false, false)
	rec1 := testutil.GetStorageRecord("http://example.com/rec1")
	rec2 := testutil.GetStorageRecord("http://example.com/rec2")

	f.SetStorageRecord(rec1)
	assert.Equal(t, 1, len(f.StorageRecords))
	assert.Equal(t, "http://example.com/rec1", f.StorageRecords[0].URL)

	f.SetStorageRecord(rec2)
	assert.Equal(t, 2, len(f.StorageRecords))
	assert.Equal(t, "http://example.com/rec2", f.StorageRecords[1].URL)

	// Reseting the a storage record should update, not append
	now := time.Now()
	rec1.StoredAt = now
	f.SetStorageRecord(rec1)
	assert.Equal(t, 2, len(f.StorageRecords))
	assert.Equal(t, now, f.StorageRecords[0].StoredAt)
}
