package testutil

import (
	"fmt"
	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"time"
)

var Bloomsday, _ = time.Parse(time.RFC3339, "1904-06-16T15:04:05Z")

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
	f.Id = 999
	f.InstitutionId = 9855
	f.IntellectualObjectId = 4432
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
		Id:                     555,
		Institution:            "test.edu",
		InstitutionId:          9855,
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

func GetStorageRecord(url string) *service.StorageRecord {
	return &service.StorageRecord{
		URL:      url,
		StoredAt: Bloomsday,
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
		ETag:                   "86753098675309",
		Id:                     0,
		Identifier:             "test.edu/TestBag001",
		Institution:            "test.edu",
		InstitutionId:          0,
		SourceOrganization:     "Test Univerisity Library",
		State:                  constants.StateActive,
		StorageOption:          constants.StorageStandard,
		Title:                  "Test Bag from Factory",
	}
}
