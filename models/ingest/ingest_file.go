package ingest

import (
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"strings"
	"time"
)

type IngestFile struct {
	Checksums         []*IngestChecksum `json:"checksums"`
	ErrorMessage      string            `json:"ingesterror_message,omitempty"`
	ExistingVersionId int64             `json:"existing_version_id,omitempty"`
	FileCreated       time.Time         `json:"file_created,omitempty"`
	Format            string            `json:"file_format,omitempty"`
	Gid               int               `json:"file_gid,omitempty"`
	Gname             string            `json:"file_gname,omitempty"`
	Id                int               `json:"id,omitempty"`
	Identifier        string            `json:"identifier,omitempty"`
	Mode              int64             `json:"file_mode,omitempty"`
	Modified          time.Time         `json:"file_modified,omitempty"`
	NeedsSave         bool              `json:"needs_save,omitempty"`
	PathInBag         string            `json:"path_in_bag,omitempty"`
	Size              int64             `json:"size,omitempty"`
	State             string            `json:"state,omitempty"`
	StorageOption     string            `json:"storage_option"`
	StorageRecords    []*StorageRecord  `json:"storage_records"`
	UUID              string            `json:"uuid,omitempty"`
	Uid               int               `json:"file_uid,omitempty"`
	Uname             string            `json:"file_uname,omitempty"`
}

func NewIngestFile(objIdentifier, pathInBag string) *IngestFile {
	return &IngestFile{
		Checksums:         make([]*IngestChecksum, 0),
		ExistingVersionId: 0,
		Identifier:        fmt.Sprintf("%s/%s", objIdentifier, pathInBag),
		NeedsSave:         true,
		PathInBag:         pathInBag,
		State:             "A",
		StorageOption:     "Standard",
		StorageRecords:    make([]*StorageRecord, 0),
	}
}

// Returns the type of bag file: payload_file, manifest, tag_manifest,
// or tag_file.
func (f *IngestFile) FileType() string {
	fileType := constants.FileTypePayloadFile
	if strings.HasPrefix(f.PathInBag, "tagmanifest-") {
		fileType = constants.FileTypeTagManifest
	} else if strings.HasPrefix(f.PathInBag, "manifest-") {
		fileType = constants.FileTypeManifest
	} else {
		fileType = constants.FileTypeTagFile
	}
	return fileType
}

// Returns true if this is a tag file that we should try to parse.
func (f *IngestFile) IsParsableTagFile() bool {
	return (f.PathInBag == "bagit.txt" ||
		f.PathInBag == "bag-info.txt" ||
		f.PathInBag == "aptrust-info.txt")
}

// Sets a checksum value for this file. If the value already
// exists, this updates it; otherwise, it appends the new checksum
// to the Checksums list.
func (f *IngestFile) SetChecksum(checksum *IngestChecksum) {
	updated := false
	for i, cs := range f.Checksums {
		if cs.Source == checksum.Source && cs.Algorithm == checksum.Algorithm {
			f.Checksums[i] = checksum
			updated = true
		}
	}
	if updated == false {
		f.Checksums = append(f.Checksums, checksum)
	}
}

// Returns the checksum with the specified source and algorithm,
// or nil if the checksum isn't present.
func (f *IngestFile) GetChecksum(source, algorithm string) *IngestChecksum {
	for _, cs := range f.Checksums {
		if cs.Source == source && cs.Algorithm == algorithm {
			return cs
		}
	}
	return nil
}
