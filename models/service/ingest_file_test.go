package service_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
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
	f, err := service.IngestFileFromJson(IngestFileJson)
	assert.Nil(t, err)
	assert.Equal(t, expectedFile.Checksums, f.Checksums)
	assert.Equal(t, expectedFile.ObjectIdentifier, f.ObjectIdentifier)
	assert.Equal(t, expectedFile.PathInBag, f.PathInBag)
}

func TestFileToJson(t *testing.T) {
	f := testutil.GetIngestFile(true, true)
	data, err := f.ToJson()
	assert.Nil(t, err)
	assert.Equal(t, IngestFileJson, data)
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

func TestIdentifierIsLegal(t *testing.T) {
	ingestFile := service.NewIngestFile(testutil.ObjIdentifier, "data/legal.txt")
	ok, err := ingestFile.IdentifierIsLegal()
	assert.True(t, ok)
	assert.Nil(t, err)

	badFile := service.NewIngestFile(testutil.ObjIdentifier, "data/illegal_\u007F.txt")
	ok, err = badFile.IdentifierIsLegal()
	assert.False(t, ok)
	require.NotNil(t, err)
	assert.Equal(t, "File name 'data/illegal_\u007F.txt' contains one or more illegal control characters", err.Error())
}

func TestManifestChecksumRequired(t *testing.T) {
	f := service.NewIngestFile(testutil.ObjIdentifier, "data/legal.txt")

	// Required because payload file MUST appear in payload manifest
	ok := f.ManifestChecksumRequired("manifest-sha256.txt")
	assert.True(t, ok)

	// Not required because payload file must not be in tag manifest.
	ok = f.ManifestChecksumRequired("tagmanifest-sha256.txt")
	assert.False(t, ok)

	// Required because manifest checksum MUST appear in tag manifest
	f = service.NewIngestFile(testutil.ObjIdentifier, "manifest-md5.txt")
	ok = f.ManifestChecksumRequired("tagmanifest-sha256.txt")
	assert.True(t, ok)

	// Not required. Payload manifest does not need to appear in
	// payload manifest.
	ok = f.ManifestChecksumRequired("manifest-sha256.txt")
	assert.False(t, ok)

	// Not required. Tag manifest checksum does not have to appear
	// in any manifest.
	f = service.NewIngestFile(testutil.ObjIdentifier, "tagmanifest-md5.txt")
	ok = f.ManifestChecksumRequired("manifest-sha256.txt")
	assert.False(t, ok)

	ok = f.ManifestChecksumRequired("tagmanifest-sha256.txt")
	assert.False(t, ok)

	// Not required because tag file must not appear in payload manifest
	f = service.NewIngestFile(testutil.ObjIdentifier, "tag-file.txt")
	ok = f.ManifestChecksumRequired("manifest-sha256.txt")
	assert.False(t, ok)

	// Not required because tag file may appear in tag manifest but
	// does not have to.
	ok = f.ManifestChecksumRequired("manifest-sha256.txt")
	assert.False(t, ok)

	// Panic, because the filename param is neither a manifest nor
	// a tag manifest.
	assert.Panics(t, func() {
		f.ManifestChecksumRequired("some-random-file.txt")
	})
}

func TestChecksumsMatch(t *testing.T) {
	allChecksums := testutil.GetIngestChecksumSet()
	f := service.NewIngestFile(testutil.ObjIdentifier, "data/image.jpg")
	f.Checksums = allChecksums

	manifests := []string{
		"manifest-md5.txt",
		"manifest-sha256.txt",
	}

	// These all match...
	for _, manifest := range manifests {
		ok, err := f.ChecksumsMatch(manifest)
		assert.True(t, ok)
		assert.Nil(t, err)
	}

	// Change the ingest digests, so they don't match
	// what's in the manifests.
	f.Checksums[1].Digest = "00000"
	f.Checksums[3].Digest = "00000"

	for _, manifest := range manifests {
		ok, err := f.ChecksumsMatch(manifest)
		assert.False(t, ok)
		require.NotNil(t, err)
		assert.True(t, strings.Contains(err.Error(),
			"doesn't match manifest checksum"))
	}

	// Remove the manifest checksums entirely, but keep the
	// checksums calculated by the ingest process.
	// The error should say that the file is not in the manifest.
	f.Checksums = []*service.IngestChecksum{
		allChecksums[1],
		allChecksums[3],
	}

	for _, manifest := range manifests {
		ok, err := f.ChecksumsMatch(manifest)
		assert.False(t, ok)
		require.NotNil(t, err)
		assert.True(t, strings.Contains(err.Error(), "is not in manifest"))
	}

	// Remove the checksums calculated by the ingest process.
	// The error should say that the file is missing from the bag.
	f.Checksums = []*service.IngestChecksum{
		allChecksums[0],
		allChecksums[2],
	}

	for _, manifest := range manifests {
		ok, err := f.ChecksumsMatch(manifest)
		assert.False(t, ok)
		require.NotNil(t, err)
		assert.True(t, strings.Contains(err.Error(), "is missing from bag"))
	}
}

func TestURI(t *testing.T) {
	f := testutil.GetIngestFile(true, true)
	assert.Equal(t, "https://example.com/storage/record/1", f.URI())
}

func TestToGenericFile(t *testing.T) {
	f := testutil.GetIngestFile(true, true)
	gf := f.ToGenericFile()

	assert.Equal(t, "text/javascript", gf.FileFormat)
	assert.Equal(t, testutil.Bloomsday, gf.FileModified)
	assert.Equal(t, f.Id, gf.Id)
	assert.Equal(t, "test.edu/some-bag/data/text/file.txt", gf.Identifier)
	assert.Equal(t, 9855, gf.InstitutionId)
	assert.Equal(t, 4432, gf.IntellectualObjectId)
	assert.Equal(t, "test.edu/some-bag", gf.IntellectualObjectIdentifier)
	assert.Equal(t, int64(5555), gf.Size)
	assert.Equal(t, constants.StateActive, gf.State)
	assert.Equal(t, constants.StorageStandard, gf.StorageOption)
	assert.Equal(t, "https://example.com/storage/record/1", gf.URI)
}

func TestFidoSafeName(t *testing.T) {
	f := &service.IngestFile{
		UUID:      "209b478a-95cd-4217-b0a3-c80e3e7a2f0e",
		PathInBag: "data/docs/blah/blah/blah/somefile.pdf",
	}
	assert.Equal(t, "209b478a-95cd-4217-b0a3-c80e3e7a2f0e.pdf", f.FidoSafeName())
}

const IngestFileJson = `{"checksums":[{"algorithm":"md5","datetime":"0001-01-01T00:00:00Z","digest":"md5:ingest","source":"ingest"},{"algorithm":"md5","datetime":"0001-01-01T00:00:00Z","digest":"md5:registry","source":"registry"}],"copied_to_staging_at":"0001-01-01T00:00:00Z","error_message":"no error","file_format":"text/javascript","format_identified_at":"0001-01-01T00:00:00Z","file_modified":"1904-06-16T15:04:05Z","id":999,"institution_id":9855,"intellectual_object_id":4432,"needs_save":true,"object_identifier":"test.edu/some-bag","path_in_bag":"data/text/file.txt","size":5555,"storage_option":"Standard","storage_records":[{"url":"https://example.com/storage/record/1","stored_at":"1904-06-16T15:04:05Z"},{"url":"https://example.com/storage/record/2","stored_at":"1904-06-16T15:04:05Z"}],"uuid":"00000000-0000-0000-0000-000000000000"}`
