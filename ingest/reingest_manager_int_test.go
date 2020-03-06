// +build integration

package ingest_test

import (
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Existing ids, loaded in Pharos integration test fixture data
const ObjIdExists = "institution2.edu/toads"
const FileIdExists = "institution2.edu/coal/doc3"

func SetupReingest(t *testing.T) {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	g := ingest.NewMetadataGatherer(context, 9999, obj)
	err := g.ScanBag()
	require.Nil(t, err)
}

func GetReingestManager() *ingest.ReingestManager {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	return ingest.NewReingestManager(context, 9999, obj)
}

func TestNewReingestManager(t *testing.T) {
	manager := GetReingestManager()
	assert.NotNil(t, manager.Context)
	assert.Equal(t, "example.edu.tagsample_good.tar", manager.IngestObject.S3Key)
	assert.Equal(t, 9999, manager.WorkItemId)
}

func TestObjectWasPreviouslyIngested(t *testing.T) {
	manager := GetReingestManager()

	// Set these properties on our test object so that
	// IngestObject.Indentifier() resolves to
	// "institution2.edu/toads", which does exist as part
	// of the Pharos fixture data.
	manager.IngestObject.S3Key = "toads.tar"
	manager.IngestObject.Institution = "institution2.edu"
	wasIngested, err := manager.ObjectWasPreviouslyIngested()
	require.Nil(t, err)
	assert.True(t, wasIngested)

	// Set this so Identifier() resolves to
	// "institution.edu/bag-does-not-exist"
	manager.IngestObject.S3Key = "bag-does-not-exist.tar"
	wasIngested, err = manager.ObjectWasPreviouslyIngested()
	require.Nil(t, err)
	assert.False(t, wasIngested)
}

func TestSetStorageOption(t *testing.T) {
	genericFile := &registry.GenericFile{
		State:         "D",
		StorageOption: constants.StorageGlacierOH,
	}
	ingestFile := &service.IngestFile{
		StorageOption: constants.StorageStandard,
	}

	// StorageOption should NOT change if GenericFile is deleted.
	manager := GetReingestManager()
	manager.SetStorageOption(ingestFile, genericFile)
	assert.Equal(t, constants.StorageStandard, ingestFile.StorageOption)

	// StorageOption should change if GenericFile is active.
	genericFile.State = "A"
	manager.SetStorageOption(ingestFile, genericFile)
	assert.Equal(t, constants.StorageGlacierOH, ingestFile.StorageOption)
}

func TestFlagForUpdate(t *testing.T) {
	pharosUUID := "c445c30b-2299-4796-b803-e3c6ee43a2ae"
	genericFile := &registry.GenericFile{
		URI: fmt.Sprintf("https://example.com/storage/%s", pharosUUID),
	}
	ingestFile := &service.IngestFile{
		NeedsSave: false,
		UUID:      "c4ddee73-cbae-4f4e-a93b-ffcb0a0f2e99",
	}
	manager := GetReingestManager()
	manager.FlagForUpdate(ingestFile, genericFile)
	assert.True(t, ingestFile.NeedsSave)
	assert.Equal(t, pharosUUID, ingestFile.UUID)
}

func TestFlagUnchanged(t *testing.T) {
	pharosUUID := "c445c30b-2299-4796-b803-e3c6ee43a2ae"
	genericFile := &registry.GenericFile{
		URI: fmt.Sprintf("https://example.com/storage/%s", pharosUUID),
	}
	ingestFile := &service.IngestFile{
		NeedsSave: true,
		UUID:      "c4ddee73-cbae-4f4e-a93b-ffcb0a0f2e99",
	}
	manager := GetReingestManager()
	manager.FlagUnchanged(ingestFile, genericFile)
	assert.False(t, ingestFile.NeedsSave)
	assert.Equal(t, pharosUUID, ingestFile.UUID)
}

func TestChecksumChanged(t *testing.T) {
	pharosChecksums := make(map[string]*registry.Checksum)
	pharosChecksums[constants.AlgMd5] = &registry.Checksum{
		Algorithm: constants.AlgMd5,
		Digest:    "12345",
	}
	pharosChecksums[constants.AlgSha256] = &registry.Checksum{
		Algorithm: constants.AlgSha256,
		Digest:    "54321",
	}

	ingestFile := testutil.GetIngestFile(false, false)
	ingestFile.SetChecksum(&service.IngestChecksum{
		Algorithm: constants.AlgMd5,
		Source:    constants.SourceIngest,
		Digest:    "12345",
	})
	ingestFile.SetChecksum(&service.IngestChecksum{
		Algorithm: constants.AlgSha256,
		Source:    constants.SourceIngest,
		Digest:    "54321",
	})

	manager := GetReingestManager()

	// Ingest checksums match registry checksums,
	// so this this should return false.
	assert.False(t, manager.ChecksumChanged(ingestFile, pharosChecksums))

	// Change one md5 checksum, and we should get true.
	pharosChecksums[constants.AlgMd5].Digest = "99999"
	assert.True(t, manager.ChecksumChanged(ingestFile, pharosChecksums))

	// Fix the md5 and make sure we catch the changed sha256
	pharosChecksums[constants.AlgMd5].Digest = "12345"
	pharosChecksums[constants.AlgSha256].Digest = "99999"
	assert.True(t, manager.ChecksumChanged(ingestFile, pharosChecksums))

	// Delete the ingest sha256 and make sure missing checksum
	// causes no error. Both md5s are the same now.
	ingestFile.Checksums = ingestFile.Checksums[0:0]
	assert.False(t, manager.ChecksumChanged(ingestFile, pharosChecksums))
}

func TestGetNewest(t *testing.T) {
	t1, _ := time.Parse(time.RFC3339, "2020-01-01T12:00:00Z")
	t2, _ := time.Parse(time.RFC3339, "2020-01-02T12:00:00Z")
	t3, _ := time.Parse(time.RFC3339, "2020-01-03T12:00:00Z")
	t4, _ := time.Parse(time.RFC3339, "2020-01-04T12:00:00Z")
	t5, _ := time.Parse(time.RFC3339, "2020-01-05T12:00:00Z")
	t6, _ := time.Parse(time.RFC3339, "2020-01-06T12:00:00Z")
	checksums := []*registry.Checksum{
		&registry.Checksum{
			Id:        1,
			Algorithm: constants.AlgMd5,
			DateTime:  t1,
		},
		&registry.Checksum{
			Id:        2,
			Algorithm: constants.AlgMd5,
			DateTime:  t2,
		},
		&registry.Checksum{
			Id:        3,
			Algorithm: constants.AlgSha256,
			DateTime:  t3,
		},
		&registry.Checksum{
			Id:        4,
			Algorithm: constants.AlgSha256,
			DateTime:  t4,
		},
		&registry.Checksum{
			Id:        5,
			Algorithm: constants.AlgSha512,
			DateTime:  t5,
		},
		&registry.Checksum{
			Id:        6,
			Algorithm: constants.AlgSha512,
			DateTime:  t6,
		},
	}
	manager := GetReingestManager()

	// For each algorithm, we should get the checksum
	// with the latest DateTime.
	newest := manager.GetNewest(checksums)
	assert.Equal(t, 2, newest[constants.AlgMd5].Id)
	assert.Equal(t, 4, newest[constants.AlgSha256].Id)
	assert.Equal(t, 6, newest[constants.AlgSha512].Id)
}
