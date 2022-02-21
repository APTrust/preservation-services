//go:build integration

package ingest_test

import (
	"strings"
	"testing"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Existing ids, loaded in Registry integration test fixture data
const ObjIdExists = "institution2.edu/toads"
const FileIdExists = "institution2.edu/coal/doc3"

const TestBagWorkItemId = 31337

// The following the pathInBag of tag files that do not
// appear in any tag manifest. This is legal according
// to the BagIt spec. Par of our test bag
// example.edu.tag_sample_good.tar.
const UntrackedTagFile = "custom_tags/untracked_tag_file.txt"
const JunkFile = "junk_file.txt"

// This function scans bag example.edu.tagsample_good.tar
// and saves the ingest metadata in Redis.
func PutBagMetadataInRedis(t *testing.T) *service.IngestObject {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	g := ingest.NewMetadataGatherer(context, TestBagWorkItemId, obj)
	fileCount, errors := g.Run()
	assert.True(t, fileCount > 0)
	require.Empty(t, errors)

	// At this point, the IngestObject has tag values
	// parsed from the actual bag.
	return g.IngestObject
}

func PutBagMetadataInRegistry(t *testing.T, obj *service.IngestObject) {
	context := common.NewContext()

	// Get the correct institution id from Registry
	inst := context.RegistryClient.InstitutionByIdentifier("example.edu").Institution()
	require.NotNil(t, inst)
	obj.InstitutionID = inst.ID

	// Save the intel obj in Registry
	resp := context.RegistryClient.IntellectualObjectSave(obj.ToIntellectualObject())
	require.Nil(t, resp.Error)
	intelObj := resp.IntellectualObject()
	require.NotNil(t, intelObj)

	fileMap, _, err := context.RedisClient.GetBatchOfFileKeys(
		TestBagWorkItemId,
		0,
		int64(50))
	require.Nil(t, err)
	for _, ingestFile := range fileMap {
		ingestFile.InstitutionID = inst.ID
		ingestFile.IntellectualObjectID = intelObj.ID

		ingestFile.StorageRecords = []*service.StorageRecord{
			&service.StorageRecord{
				URL:        "https://example.com/" + ingestFile.UUID,
				StoredAt:   testutil.Bloomsday,
				VerifiedAt: testutil.Bloomsday,
			},
			&service.StorageRecord{
				URL:        "https://other.example.com/" + ingestFile.UUID,
				StoredAt:   testutil.Bloomsday,
				VerifiedAt: testutil.Bloomsday,
			},
		}

		genericFile, err := ingestFile.ToGenericFile()
		require.Nil(t, err)
		resp = context.RegistryClient.GenericFileSave(genericFile)
		require.Nil(t, resp.Error)
		gf := resp.GenericFile()
		require.NotNil(t, gf)
		require.NotEqual(t, 0, gf.ID)
	}
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
	assert.EqualValues(t, 9999, manager.WorkItemID)
}

func TestGetExistingObject(t *testing.T) {
	manager := GetReingestManager()

	// Set these properties on our test object so that
	// IngestObject.Indentifier() resolves to
	// "institution2.edu/toads", which does exist as part
	// of the Registry fixture data.
	manager.IngestObject.S3Key = "toads.tar"
	manager.IngestObject.Institution = "institution2.edu"
	intelObj, err := manager.GetExistingObject()
	require.Nil(t, err)
	assert.NotNil(t, intelObj)

	// Set this so Identifier() resolves to
	// "institution.edu/bag-does-not-exist"
	manager.IngestObject.S3Key = "bag-does-not-exist.tar"
	manager.IngestObject.ID = 0
	manager.IngestObject.IsReingest = false
	intelObj, err = manager.GetExistingObject()
	require.Nil(t, err)
	assert.Nil(t, intelObj)

	assert.EqualValues(t, 0, manager.IngestObject.ID)
	assert.False(t, manager.IngestObject.IsReingest)
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
	registryUUID := "c445c30b-2299-4796-b803-e3c6ee43a2ae"
	genericFile := &registry.GenericFile{
		UUID: registryUUID,
	}
	ingestFile := &service.IngestFile{
		NeedsSave: false,
		UUID:      "c4ddee73-cbae-4f4e-a93b-ffcb0a0f2e99",
	}
	manager := GetReingestManager()
	manager.FlagForUpdate(ingestFile, genericFile)
	assert.True(t, ingestFile.NeedsSave)
	assert.Equal(t, registryUUID, ingestFile.UUID)
}

func TestFlagUnchanged(t *testing.T) {
	registryUUID := "c445c30b-2299-4796-b803-e3c6ee43a2ae"
	genericFile := &registry.GenericFile{
		UUID: registryUUID,
	}
	ingestFile := &service.IngestFile{
		NeedsSave: true,
		UUID:      "c4ddee73-cbae-4f4e-a93b-ffcb0a0f2e99",
	}
	manager := GetReingestManager()
	manager.FlagUnchanged(ingestFile, genericFile)
	assert.False(t, ingestFile.NeedsSave)
	assert.Equal(t, registryUUID, ingestFile.UUID)
}

func TestChecksumChanged(t *testing.T) {
	registryChecksums := make(map[string]*registry.Checksum)
	registryChecksums[constants.AlgMd5] = &registry.Checksum{
		Algorithm: constants.AlgMd5,
		Digest:    "12345",
	}
	registryChecksums[constants.AlgSha256] = &registry.Checksum{
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
	assert.False(t, manager.ChecksumChanged(ingestFile, registryChecksums))

	// Change one md5 checksum, and we should get true.
	registryChecksums[constants.AlgMd5].Digest = "99999"
	assert.True(t, manager.ChecksumChanged(ingestFile, registryChecksums))

	// Fix the md5 and make sure we catch the changed sha256
	registryChecksums[constants.AlgMd5].Digest = "12345"
	registryChecksums[constants.AlgSha256].Digest = "99999"
	assert.True(t, manager.ChecksumChanged(ingestFile, registryChecksums))

	// Delete the ingest sha256 and make sure missing checksum
	// causes no error. Both md5s are the same now.
	ingestFile.Checksums = ingestFile.Checksums[0:0]
	assert.False(t, manager.ChecksumChanged(ingestFile, registryChecksums))
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
			ID:        1,
			Algorithm: constants.AlgMd5,
			DateTime:  t1,
		},
		&registry.Checksum{
			ID:        2,
			Algorithm: constants.AlgMd5,
			DateTime:  t2,
		},
		&registry.Checksum{
			ID:        3,
			Algorithm: constants.AlgSha256,
			DateTime:  t3,
		},
		&registry.Checksum{
			ID:        4,
			Algorithm: constants.AlgSha256,
			DateTime:  t4,
		},
		&registry.Checksum{
			ID:        5,
			Algorithm: constants.AlgSha512,
			DateTime:  t5,
		},
		&registry.Checksum{
			ID:        6,
			Algorithm: constants.AlgSha512,
			DateTime:  t6,
		},
	}
	manager := GetReingestManager()

	// For each algorithm, we should get the checksum
	// with the latest DateTime.
	newest := manager.GetNewest(checksums)
	assert.EqualValues(t, 2, newest[constants.AlgMd5].ID)
	assert.EqualValues(t, 4, newest[constants.AlgSha256].ID)
	assert.EqualValues(t, 6, newest[constants.AlgSha512].ID)
}

// This one tests all of the ReingestManager's functions,
// including the ones not explicitly covered above. Those
// include: ProcessFiles, ProcessFile, and CompareFiles.
func TestReingestManagerRun(t *testing.T) {
	obj := PutBagMetadataInRedis(t)
	PutBagMetadataInRegistry(t, obj)
	manager := GetReingestManager()
	manager.WorkItemID = TestBagWorkItemId

	wasPreviouslyIngested, errors := manager.Run()
	assert.Equal(t, 1, wasPreviouslyIngested)
	assert.Empty(t, errors, errors)

	// Test basic attributes on each ingest file.
	// Not should need saving, because they haven't changed
	// since last ingest.
	testAttrs := func(ingestFile *service.IngestFile) (errors []*service.ProcessingError) {
		testIngestFile_ReingestManager(t, ingestFile)
		assert.False(t, ingestFile.NeedsSave)
		return errors
	}
	options := service.IngestFileApplyOptions{
		MaxErrors:   1,
		MaxRetries:  1,
		RetryMs:     0,
		SaveChanges: false,
		WorkItemID:  manager.WorkItemID,
	}
	count, errors := manager.Context.RedisClient.IngestFilesApply(testAttrs, options)
	assert.Empty(t, errors, errors)
	assert.Equal(t, 16, count)

	// Create a function that alters the checksums
	// on the IngestFile records in Redis.
	changeChecksums := func(f *service.IngestFile) (errors []*service.ProcessingError) {
		for _, cs := range f.Checksums {
			cs.Digest = "00000000000000000000000000000000"
		}
		return errors
	}

	// Apply the function to alter checksums of all files
	// belonging to this work item.
	options = service.IngestFileApplyOptions{
		MaxErrors:   1,
		MaxRetries:  1,
		RetryMs:     0,
		SaveChanges: true,
		WorkItemID:  manager.WorkItemID,
	}
	count, errors = manager.Context.RedisClient.IngestFilesApply(changeChecksums, options)
	assert.Empty(t, errors, errors)
	assert.Equal(t, 16, count)

	// Now when we process the object again, it should mark all
	// files as needing save, because all have checksums that
	// do not match the Registry checksums.
	wasPreviouslyIngested, errors = manager.Run()
	assert.Equal(t, 1, wasPreviouslyIngested)
	assert.Empty(t, errors, errors)

	// Make sure these properties were set...
	assert.True(t, manager.IngestObject.ID > 0)
	assert.True(t, manager.IngestObject.IsReingest)

	// Make sure these properties were set on the IngestObject in Redis.
	ingestObj, err := manager.Context.RedisClient.IngestObjectGet(
		manager.WorkItemID, manager.IngestObject.Identifier())
	require.Nil(t, err)
	assert.True(t, ingestObj.ID > 0)
	assert.True(t, ingestObj.IsReingest)

	testFilesNeedSave := func(ingestFile *service.IngestFile) (errors []*service.ProcessingError) {
		testIngestFile_ReingestManager(t, ingestFile)
		// This is the main thing we want to test.
		// NeedsSave should have changed from false to true.
		assert.True(t, ingestFile.NeedsSave)
		return nil
	}
	options = service.IngestFileApplyOptions{
		MaxErrors:   1,
		MaxRetries:  1,
		RetryMs:     0,
		SaveChanges: false,
		WorkItemID:  manager.WorkItemID,
	}
	count, errors = manager.Context.RedisClient.IngestFilesApply(testFilesNeedSave, options)
	assert.Empty(t, errors, errors)
	assert.Equal(t, 16, count)

}

func testIngestFile_ReingestManager(t *testing.T, f *service.IngestFile) {
	testExpectedChecksums(t, f)
	assert.True(t, strings.Contains(f.FileFormat, "/"))
	assert.False(t, f.FileModified.IsZero())
	assert.NotEqual(t, 0, f.InstitutionID)
	assert.Equal(t, "example.edu/example.edu.tagsample_good", f.ObjectIdentifier)
	assert.NotEqual(t, "", f.PathInBag)
	assert.True(t, f.Size > int64(0))
	assert.Equal(t, constants.StorageStandard, f.StorageOption)
	assert.Equal(t, 36, len(f.UUID))
	assert.True(t, util.LooksLikeUUID(f.UUID), f.PathInBag)
	require.Equal(t, 0, len(f.StorageRecords), f.PathInBag)
	require.Equal(t, 2, len(f.RegistryURLs), "%s: %v", f.PathInBag, f.RegistryURLs)
}

func shouldHaveManifestChecksum(f *service.IngestFile) bool {
	return (f.PathInBag != UntrackedTagFile &&
		f.PathInBag != JunkFile &&
		f.FileType() == constants.FileTypePayload)
}

func shouldHaveTagManifestChecksum(f *service.IngestFile) bool {
	return (f.PathInBag != UntrackedTagFile &&
		f.PathInBag != JunkFile &&
		f.FileType() != constants.FileTypeTagManifest &&
		f.FileType() != constants.FileTypePayload)
}

func testExpectedChecksums(t *testing.T, f *service.IngestFile) {
	ingestMd5 := f.GetChecksum(constants.SourceIngest, constants.AlgMd5)
	ingestSha256 := f.GetChecksum(constants.SourceIngest, constants.AlgSha256)
	ingestSha512 := f.GetChecksum(constants.SourceIngest, constants.AlgSha512)
	manifestMd5 := f.GetChecksum(constants.SourceManifest, constants.AlgMd5)
	manifestSha256 := f.GetChecksum(constants.SourceManifest, constants.AlgSha256)
	tagmanifestMd5 := f.GetChecksum(constants.SourceTagManifest, constants.AlgMd5)
	tagmanifestSha256 := f.GetChecksum(constants.SourceTagManifest, constants.AlgSha256)

	testExpectedChecksum(t, f, ingestMd5)
	testExpectedChecksum(t, f, ingestSha256)
	testExpectedChecksum(t, f, ingestSha512)
	if shouldHaveManifestChecksum(f) {
		testExpectedChecksum(t, f, manifestMd5)
		testExpectedChecksum(t, f, manifestSha256)
	}
	if shouldHaveTagManifestChecksum(f) {
		testExpectedChecksum(t, f, tagmanifestMd5)
		testExpectedChecksum(t, f, tagmanifestSha256)
	}
}

func testExpectedChecksum(t *testing.T, f *service.IngestFile, cs *service.IngestChecksum) {
	require.NotNil(t, cs, f.PathInBag)
	validSources := []string{
		constants.SourceIngest,
		constants.SourceManifest,
		constants.SourceTagManifest,
	}
	assert.NotEqual(t, "", cs.Algorithm, f.PathInBag)
	assert.False(t, cs.DateTime.IsZero(), f.PathInBag)
	assert.True(t, len(cs.Digest) >= 32, f.PathInBag)
	assert.True(t, util.StringListContains(validSources, cs.Source), f.PathInBag)
}
