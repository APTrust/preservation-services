package ingest_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"path"
	"testing"
)

func getTarFileReader(t *testing.T, filename string) io.ReadCloser {
	pathToFile := testutil.PathToUnitTestBag(filename)
	reader, err := os.Open(pathToFile)
	require.Nil(t, err)
	require.NotNil(t, reader)
	return reader
}

// Call defer scanner.CloseReader()
func getScanner(t *testing.T, bagname string) *ingest.TarredBagScanner {
	obj := service.NewIngestObject("bucket", "example.edu.tagsample_good.tar", "1234", "example.edu", 9855, int64(300))
	reader := getTarFileReader(t, bagname)
	return ingest.NewTarredBagScanner(reader, obj, testutil.TempDir)
}

func TestNewTarredBagScanner(t *testing.T) {
	scanner := getScanner(t, "example.edu.tagsample_good.tar")
	assert.NotNil(t, scanner)
	assert.NotNil(t, scanner.IngestObject)
	assert.NotNil(t, scanner.TarReader)
	assert.Equal(t, testutil.TempDir, scanner.TempDir)
	assert.NotNil(t, scanner.TempFiles)
	defer scanner.CloseReader()
}

func TestProcessNextEntry(t *testing.T) {
	scanner := getScanner(t, "example.edu.tagsample_good.tar")
	require.NotNil(t, scanner)
	defer scanner.Finish()

	ingestFiles := make([]*service.IngestFile, 0)

	for {
		ingestFile, err := scanner.ProcessNextEntry()
		// EOF expected at end of file
		if err == io.EOF {
			break
		}
		// Any non-EOF error is a problem
		require.Nil(t, err)
		if ingestFile != nil {
			ingestFiles = append(ingestFiles, ingestFile)
		}
	}

	assert.Equal(t, 16, len(ingestFiles))
	assertAllFilesFound(t, ingestFiles)
	assert.Equal(t, 7, len(scanner.TempFiles))
	assertAllTempFilesExist(t, scanner.TempFiles)
}

func TestScannerFinish(t *testing.T) {
	scanner := getScanner(t, "example.edu.tagsample_good.tar")
	require.NotNil(t, scanner)
	for {
		_, err := scanner.ProcessNextEntry()
		if err == io.EOF {
			break
		}
		// Any non-EOF error is a problem
		require.Nil(t, err)
	}

	assert.Equal(t, 7, len(scanner.TempFiles))

	// Make sure all test files are deleted
	scanner.Finish()
	assertAllTempFilesDeleted(t, scanner.TempFiles)
}

func assertIngestFileComplete(t *testing.T, f *service.IngestFile) {
	require.NotNil(t, f)
	assert.Equal(t, 2, f.Checksums)
	assert.Equal(t, 0, f.StorageRecords)
	assert.Equal(t, "", f.ErrorMessage)
	assert.Equal(t, "", f.FileFormat) // TODO: This may change
	assert.Equal(t, 0, f.Id)
	assert.True(t, f.NeedsSave)
	assert.Equal(t, "example.edu/example.edu.tagsample_good.tar", f.ObjectIdentifier)
	assert.True(t, len(f.PathInBag) > 1)
	assert.Equal(t, "Standard", f.StorageOption)
	assert.Equal(t, 36, len(f.UUID))

	assertChecksumsComplete(t, f)
}

func assertChecksumsComplete(t *testing.T, f *service.IngestFile) {
	cs1 := f.Checksums[0]
	assert.Equal(t, constants.SourceIngest, cs1.Source)
	assert.Equal(t, constants.AlgMd5, cs1.Algorithm)
	assert.False(t, cs1.DateTime.IsZero())
	assert.Equal(t, 32, len(cs1.Digest))

	cs2 := f.Checksums[1]
	assert.Equal(t, constants.SourceIngest, cs2.Source)
	assert.Equal(t, constants.AlgSha256, cs2.Algorithm)
	assert.False(t, cs2.DateTime.IsZero())
	assert.Equal(t, 64, len(cs1.Digest))
}

func assertAllFilesFound(t *testing.T, files []*service.IngestFile) {
	for _, expected := range ExpectedFiles {
		found := false
		for _, f := range files {
			if f.PathInBag == expected {
				found = true
			}
		}
		assert.True(t, found, "File %s not found in bag", expected)
	}
}

func assertAllTempFilesExist(t *testing.T, tempFiles []string) {
	for _, expected := range ExpectedTempFiles {
		fullPath := path.Join(testutil.TempDir, expected)
		inList := false
		onDisk := false
		hasData := false
		for _, f := range tempFiles {
			if f == fullPath {
				inList = true
				if util.FileExists(fullPath) {
					onDisk = true
					fileStat, _ := os.Stat(fullPath)
					hasData = fileStat.Size() > 30
				}
			}
		}
		assert.True(t, inList, "Temp file %s not found in list", expected)
		assert.True(t, onDisk, "Temp file %s not found on disk", expected)
		assert.True(t, hasData, "Temp file %s has no data", expected)
	}
}

func assertAllTempFilesDeleted(t *testing.T, tempFiles []string) {
	for _, f := range tempFiles {
		assert.False(t, util.FileExists(f), "File %s should have been deleted", f)
	}
}

var ExpectedFiles = []string{
	"aptrust-info.txt",
	"bag-info.txt",
	"bagit.txt",
	"custom_tag_file.txt",
	"junk_file.txt",
	"manifest-md5.txt",
	"manifest-sha256.txt",
	"tagmanifest-md5.txt",
	"tagmanifest-sha256.txt",
	"data/datastream-DC",
	"data/datastream-descMetadata",
	"data/datastream-MARC",
	"data/datastream-RELS-EXT",
	"custom_tags/tracked_file_custom.xml",
	"custom_tags/tracked_tag_file.txt",
	"custom_tags/untracked_tag_file.txt",
}

// Manifests, TagManifests, and selected tag files
var ExpectedTempFiles = []string{
	"aptrust-info.txt",
	"bag-info.txt",
	"bagit.txt",
	"manifest-md5.txt",
	"manifest-sha256.txt",
	"tagmanifest-md5.txt",
	"tagmanifest-sha256.txt",
}
