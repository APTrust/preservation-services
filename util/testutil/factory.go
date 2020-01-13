package testutil

import (
	"fmt"
	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/service"
	"time"
)

var Timestamp, _ = time.Parse(time.RFC3339, "2020-01-02T15:04:05Z")

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
	f.Id = 999
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

	}
	return f
}

func GetIngestObject() *service.IngestObject {
	return &service.IngestObject{
		DeletedFromReceivingAt: Timestamp,
		ETag:                   "12345678",
		ErrorMessage:           "No error",
		Id:                     555,
		Institution:            "test.edu",
		Manifests:              []string{"manifest-md5.txt", "manifest-sha256.txt"},
		ParsableTagFiles:       []string{"bag-info.txt", "aptrust-info.txt"},
		S3Bucket:               "aptrust.receiving.test.edu",
		S3Key:                  "some-bag.tar",
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
		StoredAt: Timestamp,
	}
}
