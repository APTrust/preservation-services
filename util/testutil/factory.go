package testutil

import (
	"fmt"
	"time"

	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	uuid "github.com/satori/go.uuid"
)

var Bloomsday, _ = time.Parse(time.RFC3339, "1904-06-16T15:04:05Z")
var EmptyMd5 = "00000000000000000000000000000000"
var EmptySha256 = EmptyMd5 + EmptyMd5

const (
	Institution   = "test.edu"
	ObjIdentifier = "test.edu/test-bag"
	PathInBag     = ""
	StorageURL    = "https://s3.example.com/preservation/54321"
)

func GetIngestChecksum(alg, source string) *service.IngestChecksum {
	return &service.IngestChecksum{
		Algorithm: alg,
		Source:    source,
		Digest:    fmt.Sprintf("%s:%s", alg, source),
	}
}

func GetIngestFile(withChecksums, withStorageRecords bool) *service.IngestFile {
	f := service.NewIngestFile(ObjIdentifier, "data/image.jpg")
	f.ErrorMessage = "no error"
	f.FileFormat = "text/javascript"
	f.FileModified = Bloomsday
	f.ID = 999
	f.InstitutionID = 9855
	f.IntellectualObjectID = 4432
	f.ObjectIdentifier = "test.edu/some-bag"
	f.PathInBag = "data/text/file.txt"
	f.Size = 5555
	f.StorageOption = "Standard"
	f.UUID = constants.EmptyUUID
	if withChecksums {
		f.SetChecksum(GetIngestChecksum(constants.AlgMd5, constants.SourceIngest))
		f.SetChecksum(GetIngestChecksum(constants.AlgMd5, constants.SourceRegistry))
	}
	if withStorageRecords {
		f.StorageRecords = append(
			f.StorageRecords,
			&service.StorageRecord{
				URL:      "https://example.com/storage/record/1",
				StoredAt: Bloomsday,
			})
		f.StorageRecords = append(
			f.StorageRecords,
			&service.StorageRecord{
				URL:      "https://example.com/storage/record/2",
				StoredAt: Bloomsday,
			})
	}
	return f
}

func GetIngestObject() *service.IngestObject {
	return &service.IngestObject{
		DeletedFromReceivingAt: Bloomsday,
		ETag:                   "12345678",
		ErrorMessage:           "No error",
		ID:                     555,
		Institution:            "test.edu",
		InstitutionID:          9855,
		Manifests:              []string{"manifest-md5.txt", "manifest-sha256.txt"},
		ParsableTagFiles:       []string{"bag-info.txt", "aptrust-info.txt"},
		S3Bucket:               "aptrust.receiving.test.edu",
		S3Key:                  "some-bag.tar",
		Serialization:          "application/tar",
		Size:                   99999,
		StorageOption:          "Standard",
		TagFiles:               []string{"bag-info.txt", "aptrust-info.txt", "misc/custom-tag-file.txt"},
		TagManifests:           []string{"tagmanifest-md5.txt", "tagmanifest-sha256.txt"},
		Tags:                   make([]*bagit.Tag, 0),
	}
}

func GetStorageRecord(provider, bucket, url string) *service.StorageRecord {
	return &service.StorageRecord{
		Bucket:   bucket,
		Provider: provider,
		StoredAt: Bloomsday,
		URL:      url,
	}
}

// GetIngestChecksumSet returns two pairs of checksums.
// The first pair contains an md5 from the bag manifest
// and an md5 calculated by the ingest process. The second
// pair is a sha256 from the manifest and one calculated by
// the ingest process. In each pair, the digests match.
func GetIngestChecksumSet() []*service.IngestChecksum {
	now := time.Now().UTC()
	return []*service.IngestChecksum{
		&service.IngestChecksum{
			Algorithm: "md5",
			DateTime:  now,
			Digest:    "12345",
			Source:    constants.SourceManifest,
		},
		&service.IngestChecksum{
			Algorithm: "md5",
			DateTime:  now,
			Digest:    "12345",
			Source:    constants.SourceIngest,
		},
		&service.IngestChecksum{
			Algorithm: "sha256",
			DateTime:  now,
			Digest:    "12345",
			Source:    constants.SourceManifest,
		},
		&service.IngestChecksum{
			Algorithm: "sha256",
			DateTime:  now,
			Digest:    "12345",
			Source:    constants.SourceIngest,
		},
	}
}

func GetIntellectualObject() *registry.IntellectualObject {
	return &registry.IntellectualObject{
		Access:                 constants.AccessInstitution,
		AltIdentifier:          "AltIdentifier001",
		BagGroupIdentifier:     "BagGroup001",
		BagItProfileIdentifier: constants.DefaultProfileIdentifier,
		BagName:                "TestBag001",
		Description:            "Test bag from factory",
		FileCount:              21,
		FileSize:               543210000,
		ETag:                   "86753098675309",
		ID:                     0,
		Identifier:             "test.edu/TestBag001",
		Institution:            "test.edu",
		InstitutionID:          0,
		SourceOrganization:     "Test Univerisity Library",
		State:                  constants.StateActive,
		StorageOption:          constants.StorageStandard,
		Title:                  "Test Bag from Factory",
	}
}

func GetGenericFileForObj(obj *registry.IntellectualObject, suffix int, withChecksums, withEvents bool) *registry.GenericFile {
	gf := &registry.GenericFile{
		FileFormat:                   "text/plain",
		FileModified:                 Bloomsday,
		ID:                           0,
		Identifier:                   fmt.Sprintf("%s/object/data/file_%d.txt", obj.Identifier, suffix),
		InstitutionID:                obj.InstitutionID,
		IntellectualObjectID:         obj.ID,
		IntellectualObjectIdentifier: obj.Identifier,
		Size:                         484896,
		State:                        constants.StateActive,
		StorageOption:                constants.StorageStandard,
		URI:                          fmt.Sprintf("https://example.com/00000000%d", suffix),
	}
	if withChecksums {
		gf.Checksums = []*registry.Checksum{
			GetChecksum(gf, constants.AlgMd5),
			GetChecksum(gf, constants.AlgSha256),
		}
	}
	if withEvents {
		gf.PremisEvents = []*registry.PremisEvent{
			GetPremisEvent(gf, constants.EventAccessAssignment),
			GetPremisEvent(gf, constants.EventDigestCalculation),
			GetPremisEvent(gf, constants.EventIdentifierAssignment),
			GetPremisEvent(gf, constants.EventIngestion),
			GetPremisEvent(gf, constants.EventReplication),
		}
	}
	return gf
}

func GetChecksum(gf *registry.GenericFile, alg string) *registry.Checksum {
	return &registry.Checksum{
		Algorithm:     alg,
		DateTime:      Bloomsday,
		Digest:        "0000000099999999",
		GenericFileID: gf.ID,
	}
}

func GetPremisEvent(gf *registry.GenericFile, eventType string) *registry.PremisEvent {
	return &registry.PremisEvent{
		Agent:                        "Maxwell Smart",
		DateTime:                     Bloomsday,
		Detail:                       "Fake event detail",
		EventType:                    eventType,
		GenericFileID:                gf.ID,
		GenericFileIdentifier:        gf.Identifier,
		Identifier:                   uuid.NewV4().String(),
		InstitutionID:                gf.InstitutionID,
		IntellectualObjectID:         gf.IntellectualObjectID,
		IntellectualObjectIdentifier: gf.IntellectualObjectIdentifier,
		Object:                       "Fake event object",
		OutcomeDetail:                constants.OutcomeSuccess,
		OutcomeInformation:           "Fake outcome information",
		Outcome:                      "Fake outcome",
	}
}

func GetRestorationObject() *service.RestorationObject {
	return &service.RestorationObject{
		AllFilesRestored:       true,
		BagItProfileIdentifier: constants.DefaultProfileIdentifier,
		ErrorMessage:           "No error",
		Identifier:             "test.edu/bag-name.tar",
		RestoredAt:             Bloomsday,
		RestorationSource:      constants.RestorationSourceS3,
		RestorationTarget:      "aptrust.restore.test.edu",
		RestorationType:        constants.RestorationTypeObject,
		URL:                    "https://s3.example.com/restore-bucket/bag-name.tar",
	}
}
