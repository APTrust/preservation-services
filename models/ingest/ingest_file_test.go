package ingest_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/ingest"
	"github.com/stretchr/testify/assert"
	"testing"
)

const objIdentifier = "test.edu/test-bag"

func TestNewIngestFile(t *testing.T) {
	f := ingest.NewIngestFile(objIdentifier, "data/image.jpg")
	assert.NotNil(t, f.Checksums)
	assert.EqualValues(t, 0, f.Id)
	assert.Equal(t, objIdentifier, f.ObjectIdentifier)
	assert.True(t, f.NeedsSave)
	assert.Equal(t, "data/image.jpg", f.PathInBag)
	assert.Equal(t, "Standard", f.StorageOption)
	assert.NotNil(t, f.StorageRecords)
}

func TestIdentifier(t *testing.T) {
	f := ingest.NewIngestFile(objIdentifier, "data/image.jpg")
	assert.Equal(t, "test.edu/test-bag/data/image.jpg", f.Identifier())
}

func TestFileType(t *testing.T) {
	f := ingest.NewIngestFile(objIdentifier, "data/image.jpg")
	assert.Equal(t, constants.FileTypePayload, f.FileType())

	f.PathInBag = "manifest-md5.txt"
	assert.Equal(t, constants.FileTypeManifest, f.FileType())

	f.PathInBag = "tagmanifest-sha256.txt"
	assert.Equal(t, constants.FileTypeTagManifest, f.FileType())

	f.PathInBag = "bag-info.txt"
	assert.Equal(t, constants.FileTypeTag, f.FileType())

	f.PathInBag = "custom-tags/somefile.txt"
	assert.Equal(t, constants.FileTypeTag, f.FileType())
}

func TestIsParsableTagFile(t *testing.T) {
	f := ingest.NewIngestFile(objIdentifier, "data/image.jpg")
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

func getFileAndChecksums() (*ingest.IngestFile, *ingest.IngestChecksum, *ingest.IngestChecksum) {
	f := ingest.NewIngestFile(objIdentifier, "data/image.jpg")
	firstMd5 := &ingest.IngestChecksum{
		Algorithm: constants.AlgMd5,
		Source:    constants.SourceIngest,
		Digest:    "first",
	}
	secondMd5 := &ingest.IngestChecksum{
		Algorithm: constants.AlgMd5,
		Source:    constants.SourceRegistry,
		Digest:    "second",
	}
	return f, firstMd5, secondMd5
}

func TestSetChecksum(t *testing.T) {
	f, firstMd5, secondMd5 := getFileAndChecksums()

	f.SetChecksum(firstMd5)
	assert.Equal(t, 1, len(f.Checksums))
	assert.Equal(t, "first", f.Checksums[0].Digest)

	f.SetChecksum(secondMd5)
	assert.Equal(t, 2, len(f.Checksums))
	assert.Equal(t, "second", f.Checksums[1].Digest)

	// Reseting the a checksum should update, not append
	firstMd5.Digest = "first-updated"
	f.SetChecksum(firstMd5)
	assert.Equal(t, 2, len(f.Checksums))
	assert.Equal(t, "first-updated", f.Checksums[0].Digest)
}

func TestGetChecksum(t *testing.T) {
	f, firstMd5, secondMd5 := getFileAndChecksums()
	f.SetChecksum(firstMd5)
	f.SetChecksum(secondMd5)

	ingestMd5 := f.GetChecksum(constants.SourceIngest, constants.AlgMd5)
	assert.Equal(t, constants.SourceIngest, ingestMd5.Source)
	assert.Equal(t, constants.AlgMd5, ingestMd5.Algorithm)
	assert.Equal(t, "first", ingestMd5.Digest)

	registryMd5 := f.GetChecksum(constants.SourceRegistry, constants.AlgMd5)
	assert.Equal(t, constants.SourceRegistry, registryMd5.Source)
	assert.Equal(t, constants.AlgMd5, registryMd5.Algorithm)
	assert.Equal(t, "second", registryMd5.Digest)

	nilChecksum := f.GetChecksum(constants.SourceManifest, constants.AlgSha256)
	assert.Nil(t, nilChecksum)
}
