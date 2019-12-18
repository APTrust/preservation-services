package service_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/stretchr/testify/assert"
	"testing"
)

const objIdentifier = "test.edu/test-bag"

const fileJson = `{"checksums":[{"algorithm":"md5","datetime":"0001-01-01T00:00:00Z","digest":"first","source":"ingest"},{"algorithm":"md5","datetime":"0001-01-01T00:00:00Z","digest":"second","source":"registry"}],"error_message":"no error","file_format":"text/javascript","id":999,"needs_save":true,"object_identifier":"test.edu/some-bag","path_in_bag":"data/text/file.txt","size":5555,"storage_option":"Standard","storage_records":[],"uuid":"00000000-0000-0000-0000-000000000000"}`

func TestNewIngestFile(t *testing.T) {
	f := service.NewIngestFile(objIdentifier, "data/image.jpg")
	assert.NotNil(t, f.Checksums)
	assert.EqualValues(t, 0, f.Id)
	assert.Equal(t, objIdentifier, f.ObjectIdentifier)
	assert.True(t, f.NeedsSave)
	assert.Equal(t, "data/image.jpg", f.PathInBag)
	assert.Equal(t, "Standard", f.StorageOption)
	assert.NotNil(t, f.StorageRecords)
}

func TestFileFromJson(t *testing.T) {
	expectedFile := getFile()
	f, err := service.IngestFileFromJson(fileJson)
	assert.Nil(t, err)
	assert.Equal(t, expectedFile.Checksums, f.Checksums)
	assert.Equal(t, expectedFile.ObjectIdentifier, f.ObjectIdentifier)
	assert.Equal(t, expectedFile.PathInBag, f.PathInBag)
}

func TestFileToJson(t *testing.T) {
	f := getFile()
	data, err := f.ToJson()
	assert.Nil(t, err)
	assert.Equal(t, fileJson, data)
}

func TestIdentifier(t *testing.T) {
	f := service.NewIngestFile(objIdentifier, "data/image.jpg")
	assert.Equal(t, "test.edu/test-bag/data/image.jpg", f.Identifier())
}

func TestFileType(t *testing.T) {
	f := service.NewIngestFile(objIdentifier, "data/image.jpg")
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
	f := service.NewIngestFile(objIdentifier, "data/image.jpg")
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

func getFileAndChecksums() (*service.IngestFile, *service.IngestChecksum, *service.IngestChecksum) {
	f := service.NewIngestFile(objIdentifier, "data/image.jpg")
	firstMd5 := &service.IngestChecksum{
		Algorithm: constants.AlgMd5,
		Source:    constants.SourceIngest,
		Digest:    "first",
	}
	secondMd5 := &service.IngestChecksum{
		Algorithm: constants.AlgMd5,
		Source:    constants.SourceRegistry,
		Digest:    "second",
	}
	return f, firstMd5, secondMd5
}

func getFile() *service.IngestFile {
	f, firstMd5, secondMd5 := getFileAndChecksums()
	f.SetChecksum(firstMd5)
	f.SetChecksum(secondMd5)
	f.ErrorMessage = "no error"
	f.FileFormat = "text/javascript"
	f.Id = 999
	f.ObjectIdentifier = "test.edu/some-bag"
	f.PathInBag = "data/text/file.txt"
	f.Size = 5555
	f.StorageOption = "Standard"
	f.UUID = constants.EmptyUUID
	return f
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
