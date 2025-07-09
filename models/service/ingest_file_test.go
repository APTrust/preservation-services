package service_test

import (
	"strings"
	"testing"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const eObjIdent = "test.edu/obj"
const ePathInBag = "data/file.txt"
const eFileIdent = "test.edu/obj/data/file.txt"

func TestNewIngestFile(t *testing.T) {
	f := service.NewIngestFile(testutil.ObjIdentifier, "data/image.jpg")
	assert.NotNil(t, f.Checksums)
	assert.EqualValues(t, 0, f.ID)
	assert.Equal(t, testutil.ObjIdentifier, f.ObjectIdentifier)
	assert.True(t, f.NeedsSave)
	assert.Equal(t, "data/image.jpg", f.PathInBag)
	assert.Equal(t, "Standard", f.StorageOption)
	assert.NotNil(t, f.StorageRecords)
}

func TestFileFromJson(t *testing.T) {
	expectedFile := testutil.GetIngestFile(true, true)
	f, err := service.IngestFileFromJSON(IngestFileJson)
	assert.Nil(t, err)
	assert.Equal(t, expectedFile.Checksums, f.Checksums)
	assert.Equal(t, expectedFile.ObjectIdentifier, f.ObjectIdentifier)
	assert.Equal(t, expectedFile.PathInBag, f.PathInBag)
}

func TestFileToJson(t *testing.T) {
	f := testutil.GetIngestFile(true, true)
	data, err := f.ToJSON()
	assert.Nil(t, err)
	assert.Equal(t, IngestFileJson, data)
}

func TestIdentifier(t *testing.T) {
	f := service.NewIngestFile(testutil.ObjIdentifier, "data/image.jpg")
	assert.Equal(t, "test.edu/test-bag/data/image.jpg", f.Identifier())
}

func TestInstitution(t *testing.T) {
	f := service.NewIngestFile(testutil.ObjIdentifier, "data/image.jpg")
	assert.Equal(t, "test.edu", f.Institution())
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

func TestSetAndGetStorageRecord(t *testing.T) {
	f := testutil.GetIngestFile(false, false)
	rec1 := testutil.GetStorageRecord(
		"ExampleProvider",
		"PresBucket",
		"http://example.com/rec1",
	)
	rec2 := testutil.GetStorageRecord(
		"OtherProvider",
		"OtherBucket",
		"http://example.com/other/rec2",
	)

	f.SetStorageRecord(rec1)
	assert.Equal(t, 1, len(f.StorageRecords))
	assert.Equal(t, "http://example.com/rec1", f.StorageRecords[0].URL)

	f.SetStorageRecord(rec2)
	assert.Equal(t, 2, len(f.StorageRecords))
	assert.Equal(t, "http://example.com/other/rec2", f.StorageRecords[1].URL)

	// Reseting the a storage record should update, not append
	now := time.Now()
	rec1.StoredAt = now
	f.SetStorageRecord(rec1)
	assert.Equal(t, 2, len(f.StorageRecords))
	assert.Equal(t, now, f.StorageRecords[0].StoredAt)

	retrievedRecord1 := f.GetStorageRecord(rec1.Provider, rec1.Bucket)
	require.NotNil(t, retrievedRecord1)
	assert.Equal(t, rec1.URL, retrievedRecord1.URL)

	retrievedRecord2 := f.GetStorageRecord(rec2.Provider, rec2.Bucket)
	require.NotNil(t, retrievedRecord2)
	assert.Equal(t, rec2.URL, retrievedRecord2.URL)
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
	f := getFileForEvents()
	f.NeedsSave = true
	gf, err := f.ToGenericFile()
	require.Nil(t, err)

	assert.Equal(t, "text/javascript", gf.FileFormat)
	assert.Equal(t, testutil.Bloomsday, gf.FileModified)
	assert.Equal(t, f.ID, gf.ID)
	assert.Equal(t, eFileIdent, gf.Identifier)
	assert.EqualValues(t, 1234, gf.InstitutionID)
	assert.EqualValues(t, 5678, gf.IntellectualObjectID)

	objIdentifier, err := gf.IntellectualObjectIdentifier()
	assert.Nil(t, err)
	assert.Equal(t, eObjIdent, objIdentifier)
	assert.Equal(t, int64(400), gf.Size)
	assert.Equal(t, constants.StateActive, gf.State)
	assert.Equal(t, constants.StorageStandard, gf.StorageOption)
	assert.Equal(t, constants.EmptyUUID, gf.UUID)

	require.Equal(t, 3, len(gf.Checksums))
	assert.Equal(t, constants.AlgMd5, gf.Checksums[0].Algorithm)
	assert.Equal(t, testutil.Bloomsday, gf.Checksums[0].DateTime)
	assert.Equal(t, "5555", gf.Checksums[0].Digest)
	assert.Equal(t, constants.AlgSha256, gf.Checksums[1].Algorithm)
	assert.Equal(t, testutil.Bloomsday, gf.Checksums[1].DateTime)
	assert.Equal(t, "256256", gf.Checksums[1].Digest)

	require.Equal(t, 9, len(gf.PremisEvents))
	eventTypeCounts := make(map[string]int)
	for _, event := range gf.PremisEvents {
		if _, ok := eventTypeCounts[event.EventType]; !ok {
			eventTypeCounts[event.EventType] = 0
		}
		eventTypeCounts[event.EventType]++
		assert.True(t, util.LooksLikeUUID(event.Identifier))
		assert.NotEmpty(t, event.EventType)
		assert.False(t, event.DateTime.IsZero())
		assert.NotEmpty(t, event.Detail)
		assert.NotEmpty(t, event.Outcome)
		assert.NotEmpty(t, event.OutcomeDetail)
		assert.NotEmpty(t, event.Object)
		assert.NotEmpty(t, event.Agent)
		assert.NotEmpty(t, event.OutcomeInformation)
		assert.Equal(t, f.ObjectIdentifier, event.IntellectualObjectIdentifier)
		assert.Equal(t, f.Identifier(), event.GenericFileIdentifier)
	}
	assert.Equal(t, 1, eventTypeCounts[constants.EventIngestion])
	assert.Equal(t, 2, eventTypeCounts[constants.EventFixityCheck])
	assert.Equal(t, 3, eventTypeCounts[constants.EventDigestCalculation])
	assert.Equal(t, 2, eventTypeCounts[constants.EventIdentifierAssignment])
	assert.Equal(t, 1, eventTypeCounts[constants.EventReplication])
}

func TestToGenericFile_StorageRecords(t *testing.T) {
	f := getFileForEvents()
	f.NeedsSave = true
	gf, err := f.ToGenericFile()
	require.Nil(t, err)

	require.Equal(t, 2, len(gf.StorageRecords))
	assert.Equal(t, "https://example.com/preservation/0987", gf.StorageRecords[0].URL)
	assert.Equal(t, "https://example.com/replication/0987", gf.StorageRecords[1].URL)

	// Don't add URLs that the registry already knows about.
	f.RegistryURLs = []string{
		"https://example.com/preservation/0987",
	}
	gf, err = f.ToGenericFile()
	require.Nil(t, err)

	require.Equal(t, 1, len(gf.StorageRecords))
	assert.Equal(t, "https://example.com/replication/0987", gf.StorageRecords[0].URL)

}

func TestHasPreservableName(t *testing.T) {
	no := []string{
		"bagit.txt",
		"fetch.txt",
		"manifest-sha256.txt",
		"tagmanifest-sha256.txt",
	}
	yes := []string{
		"random-tag-file.txt",
		"data/payload-file.txt",
		"data/subdir/image.jpg",
		"tag-dir/some-other-tag-file.xml",
	}
	for _, pathInBag := range no {
		f := &service.IngestFile{
			PathInBag: pathInBag,
		}
		assert.False(t, f.HasPreservableName(), pathInBag)
	}
	for _, pathInBag := range yes {
		f := &service.IngestFile{
			PathInBag: pathInBag,
		}
		assert.True(t, f.HasPreservableName(), pathInBag)
	}
}

func TestHasRegistryURL(t *testing.T) {
	ingestFile := &service.IngestFile{
		RegistryURLs: []string{
			"url1",
			"url2",
		},
	}
	assert.True(t, ingestFile.HasRegistryURL("url1"))
	assert.True(t, ingestFile.HasRegistryURL("url2"))
	assert.False(t, ingestFile.HasRegistryURL("url3"))
}

func TestNeedsSaveAt(t *testing.T) {
	provider := "example-provider"
	bucket := "example-bucket"
	f := &service.IngestFile{
		PathInBag: "data/some-file.txt",
		NeedsSave: true,
	}
	// True because name is preservavable, and there's no StorageRecord
	// for this provider/bucket combination.
	assert.True(t, f.NeedsSaveAt(provider, bucket))

	rec1 := testutil.GetStorageRecord(provider, bucket, "http://example.com/rec1")
	rec1.StoredAt = time.Time{}
	f.SetStorageRecord(rec1)

	// True because name is preservable and StorageRecord.StoredAt is empty.
	assert.True(t, f.NeedsSaveAt(provider, bucket))

	rec1.StoredAt = testutil.Bloomsday

	// False because StorageRecord.StoredAt is non-empty
	assert.False(t, f.NeedsSaveAt(provider, bucket))

	// Try a new ingest file...
	f = &service.IngestFile{
		PathInBag: "bagit.txt",
	}

	// False because file does not have preservable name
	assert.False(t, f.NeedsSaveAt(provider, bucket))

	// Try NeedsSave = false
	f = &service.IngestFile{
		PathInBag: "data/my_file.txt",
		NeedsSave: false,
	}

	// False because NeedsSave is false
	assert.False(t, f.NeedsSaveAt(provider, bucket))

}

func TestGetPutOptions(t *testing.T) {
	ingestFile := &service.IngestFile{
		FileFormat:       "image/jpeg",
		ObjectIdentifier: "example.edu/bag-of-photos",
		PathInBag:        "data/image  with   spaces&?:Junk.jpg",
	}
	ingestFile.SetChecksum(
		&service.IngestChecksum{
			Algorithm: constants.AlgMd5,
			Digest:    "12345",
			Source:    constants.SourceIngest,
		})
	ingestFile.SetChecksum(
		&service.IngestChecksum{
			Algorithm: constants.AlgSha256,
			Digest:    "98765",
			Source:    constants.SourceIngest,
		})

	for _, storageOption := range constants.StorageOptions {
		ingestFile.StorageOption = storageOption
		opts, err := ingestFile.GetPutOptions()
		require.Nil(t, err)
		assert.Equal(t, "example.edu", opts.UserMetadata["institution"])
		assert.Equal(t, "example.edu/bag-of-photos", opts.UserMetadata["bag"])
		assert.Equal(t, "12345", opts.UserMetadata["md5"])
		assert.Equal(t, "98765", opts.UserMetadata["sha256"])
		assert.Equal(t, "image/jpeg", opts.ContentType)
		assert.Equal(t, "data/image  with   spaces&?:Junk.jpg", opts.UserMetadata["bagpath"])
		assert.Equal(t, "data%2Fimage++with+++spaces%26%3F%3AJunk.jpg", opts.UserMetadata["bagpath-encoded"])
	}
}

func TestIngestFileFindEvent(t *testing.T) {
	//func (f *IngestFile) FindEvent(eventUUID string) *registry.PremisEvent {
	ingestFile := &service.IngestFile{}
	ingestFile.PremisEvents = []*registry.PremisEvent{
		{
			Identifier: "824de525-5f16-444b-8e77-3e280c25d0fb",
			Detail:     "Event One",
		},
		{
			Identifier: "780ee87f-c379-4e9b-913f-e5a71c514240",
			Detail:     "Event Two",
		},
	}
	event1 := ingestFile.FindEvent("824de525-5f16-444b-8e77-3e280c25d0fb")
	event2 := ingestFile.FindEvent("780ee87f-c379-4e9b-913f-e5a71c514240")
	event3 := ingestFile.FindEvent("aa2394ef-e6b9-4c08-a20a-8a319630c441")

	require.NotNil(t, event1)
	assert.Equal(t, "Event One", event1.Detail)

	require.NotNil(t, event2)
	assert.Equal(t, "Event Two", event2.Detail)

	assert.Nil(t, event3)
}

func getFileForEvents() *service.IngestFile {
	return &service.IngestFile{
		CopiedToStagingAt:    testutil.Bloomsday,
		FileFormat:           "text/javascript",
		FileModified:         testutil.Bloomsday,
		FormatIdentifiedAt:   testutil.Bloomsday,
		FormatIdentifiedBy:   constants.FmtIdSiegfried,
		FormatMatchType:      constants.MatchTypeExtension,
		InstitutionID:        1234,
		IntellectualObjectID: 5678,
		ObjectIdentifier:     eObjIdent,
		PathInBag:            ePathInBag,
		Size:                 400,
		StorageOption:        constants.StorageStandard,
		UUID:                 constants.EmptyUUID,
		Checksums: []*service.IngestChecksum{
			{
				Algorithm: constants.AlgMd5,
				DateTime:  testutil.Bloomsday,
				Digest:    "5555",
				Source:    constants.SourceManifest,
			},
			{
				Algorithm: constants.AlgMd5,
				DateTime:  testutil.Bloomsday,
				Digest:    "5555",
				Source:    constants.SourceIngest,
			},
			{
				Algorithm: constants.AlgSha256,
				DateTime:  testutil.Bloomsday,
				Digest:    "256256",
				Source:    constants.SourceManifest,
			},
			{
				Algorithm: constants.AlgSha256,
				DateTime:  testutil.Bloomsday,
				Digest:    "256256",
				Source:    constants.SourceIngest,
			},
			{
				Algorithm: constants.AlgSha512,
				DateTime:  testutil.Bloomsday,
				Digest:    "512512",
				Source:    constants.SourceIngest,
			},
		},
		StorageRecords: []*service.StorageRecord{
			{
				StoredAt:   testutil.Bloomsday,
				URL:        "https://example.com/preservation/0987",
				VerifiedAt: testutil.Bloomsday,
			},
			{
				StoredAt:   testutil.Bloomsday,
				URL:        "https://example.com/replication/0987",
				VerifiedAt: testutil.Bloomsday,
			},
		},
	}
}

func TestNewFileIngestEvent(t *testing.T) {
	f := getFileForEvents()
	event, err := f.NewFileIngestEvent()
	require.Nil(t, err)
	require.NotNil(t, event)

	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventIngestion, event.EventType)
	assert.Equal(t, testutil.Bloomsday, event.DateTime)
	assert.Equal(t, "Completed copy to preservation storage (00000000-0000-0000-0000-000000000000)", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "md5:5555", event.OutcomeDetail)
	assert.Equal(t, "preservation-services + Minio S3 client", event.Object)
	assert.Equal(t, eObjIdent, event.IntellectualObjectIdentifier)
	assert.Equal(t, constants.S3ClientName, event.Agent)
	assert.Equal(t, "Put using md5 checksum", event.OutcomeInformation)
	assert.EqualValues(t, 1234, event.InstitutionID)
	assert.EqualValues(t, 5678, event.IntellectualObjectID)
}

func TestNewFileFixityCheckEvent(t *testing.T) {
	f := getFileForEvents()
	cs := f.GetChecksum(constants.SourceManifest, constants.AlgMd5)
	event := f.NewFileFixityCheckEvent(cs)
	require.NotNil(t, event)

	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventFixityCheck, event.EventType)
	assert.Equal(t, testutil.Bloomsday, event.DateTime)
	assert.Equal(t, "Fixity check against registered hash", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "md5:5555", event.OutcomeDetail)
	assert.Equal(t, "Go language crypto/md5", event.Object)
	assert.Equal(t, eObjIdent, event.IntellectualObjectIdentifier)
	assert.Equal(t, "http://golang.org/pkg/crypto/md5/", event.Agent)
	assert.Equal(t, "Fixity matches", event.OutcomeInformation)
	assert.EqualValues(t, 1234, event.InstitutionID)
	assert.EqualValues(t, 5678, event.IntellectualObjectID)
}

func TestNewFileDigestEvent(t *testing.T) {
	f := getFileForEvents()
	cs := f.GetChecksum(constants.SourceIngest, constants.AlgSha256)
	event := f.NewFileDigestEvent(cs)
	require.NotNil(t, event)

	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventDigestCalculation, event.EventType)
	assert.Equal(t, testutil.Bloomsday, event.DateTime)
	assert.Equal(t, "Calculated fixity value", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "sha256:256256", event.OutcomeDetail)
	assert.Equal(t, "Go language crypto/sha256", event.Object)
	assert.Equal(t, eObjIdent, event.IntellectualObjectIdentifier)
	assert.Equal(t, "http://golang.org/pkg/crypto/sha256/", event.Agent)
	assert.Equal(t, "Calculated fixity value", event.OutcomeInformation)
	assert.EqualValues(t, 1234, event.InstitutionID)
	assert.EqualValues(t, 5678, event.IntellectualObjectID)
}

func TestNewFileIdentifierEvent(t *testing.T) {
	f := getFileForEvents()
	event, err := f.NewFileIdentifierEvent(f.Identifier(), constants.IdTypeBagAndPath)
	require.Nil(t, err)
	require.NotNil(t, event)

	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventIdentifierAssignment, event.EventType)
	assert.WithinDuration(t, time.Now().UTC(), event.DateTime, 1*time.Minute)
	assert.Equal(t, "Assigned new institution.bag/path identifier", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, f.Identifier(), event.OutcomeDetail)
	assert.Equal(t, "APTrust exchange/ingest processor", event.Object)
	assert.Equal(t, eObjIdent, event.IntellectualObjectIdentifier)
	assert.Equal(t, "https://github.com/APTrust/preservation-services", event.Agent)
	assert.Equal(t, "Assigned bag/filepath identifier", event.OutcomeInformation)
	assert.EqualValues(t, 1234, event.InstitutionID)
	assert.EqualValues(t, 5678, event.IntellectualObjectID)

	event, err = f.NewFileIdentifierEvent(f.StorageRecords[0].URL, constants.IdTypeStorageURL)
	require.Nil(t, err)
	require.NotNil(t, event)

	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventIdentifierAssignment, event.EventType)
	assert.WithinDuration(t, time.Now().UTC(), event.DateTime, 1*time.Minute)
	assert.Equal(t, "Assigned new storage URL identifier", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, f.StorageRecords[0].URL, event.OutcomeDetail)
	assert.Equal(t, "Go uuid library + Minio S3 library", event.Object)
	assert.Equal(t, eObjIdent, event.IntellectualObjectIdentifier)
	assert.Equal(t, "http://github.com/google/uuid", event.Agent)
	assert.Equal(t, "Assigned url identifier", event.OutcomeInformation)
	assert.EqualValues(t, 1234, event.InstitutionID)
	assert.EqualValues(t, 5678, event.IntellectualObjectID)

	event, err = f.NewFileIdentifierEvent("", constants.IdTypeStorageURL)
	require.Nil(t, event)
	require.NotNil(t, err)
	assert.Equal(t, "Param identifier cannot be empty.", err.Error())
}

func TestNewFileReplicationEvent(t *testing.T) {
	f := getFileForEvents()
	event, err := f.NewFileReplicationEvent(f.StorageRecords[1])
	require.Nil(t, err)
	require.NotNil(t, event)

	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventReplication, event.EventType)
	assert.Equal(t, testutil.Bloomsday, event.DateTime)
	assert.Equal(t, "Copied to replication storage and assigned replication URL identifier", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, f.StorageRecords[1].URL, event.OutcomeDetail)
	assert.Equal(t, "Go uuid library + Minio S3 library", event.Object)
	assert.Equal(t, eObjIdent, event.IntellectualObjectIdentifier)
	assert.Equal(t, "http://github.com/google/uuid", event.Agent)
	assert.Equal(t, "Replicated to secondary storage", event.OutcomeInformation)
	assert.EqualValues(t, 1234, event.InstitutionID)
	assert.EqualValues(t, 5678, event.IntellectualObjectID)

	badRecord := &service.StorageRecord{
		URL: "https://blah/blah/blah",
	}
	event, err = f.NewFileReplicationEvent(badRecord)
	require.Nil(t, event)
	require.NotNil(t, err)
	assert.Equal(t, "Replication record StoredAt cannot be empty", err.Error())

	badRecord.StoredAt = testutil.Bloomsday
	event, err = f.NewFileReplicationEvent(badRecord)
	require.Nil(t, event)
	require.NotNil(t, err)
	assert.Equal(t, "Replication record VerifiedAt cannot be empty", err.Error())
}

const IngestFileJson = `{"checksums":[{"algorithm":"md5","datetime":"0001-01-01T00:00:00Z","digest":"md5:ingest","source":"ingest"},{"algorithm":"md5","datetime":"0001-01-01T00:00:00Z","digest":"md5:registry","source":"registry"}],"copied_to_staging_at":"0001-01-01T00:00:00Z","error_message":"no error","file_format":"text/javascript","format_identified_at":"0001-01-01T00:00:00Z","file_modified":"1904-06-16T15:04:05Z","id":999,"institution_id":9855,"intellectual_object_id":4432,"is_reingest":false,"needs_save":true,"object_identifier":"test.edu/some-bag","path_in_bag":"data/text/file.txt","posix_metadata":{"uid":0,"gid":0,"uname":"","gname":"","atime":"0001-01-01T00:00:00Z","ctime":"0001-01-01T00:00:00Z","mtime":"0001-01-01T00:00:00Z","mode":0},"registry_urls":[],"saved_to_registry_at":"0001-01-01T00:00:00Z","size":5555,"storage_option":"Standard","storage_records":[{"bucket":"","error":"","etag":"","provider":"","size":0,"stored_at":"1904-06-16T15:04:05Z","url":"https://example.com/storage/record/1","verified_at":"0001-01-01T00:00:00Z"},{"bucket":"","error":"","etag":"","provider":"","size":0,"stored_at":"1904-06-16T15:04:05Z","url":"https://example.com/storage/record/2","verified_at":"0001-01-01T00:00:00Z"}],"uuid":"00000000-0000-0000-0000-000000000000"}`
