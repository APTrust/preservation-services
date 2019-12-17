package ingest

import (
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"strings"
)

type IngestFile struct {
	Checksums         []*IngestChecksum `json:"checksums"`
	ErrorMessage      string            `json:"ingesterror_message,omitempty"`
	ExistingVersionId int64             `json:"existing_version_id,omitempty"`
	FileFormat        string            `json:"file_format,omitempty"`
	NeedsSave         bool              `json:"needs_save,omitempty"`
	ObjectIdentifier  string            `json:"object_identifier,omitempty"`
	PathInBag         string            `json:"path_in_bag,omitempty"`
	Size              int64             `json:"size,omitempty"`
	StorageOption     string            `json:"storage_option"`
	StorageRecords    []*StorageRecord  `json:"storage_records"`
	UUID              string            `json:"uuid,omitempty"`
}

func NewIngestFile(objIdentifier, pathInBag string) *IngestFile {
	return &IngestFile{
		Checksums:         make([]*IngestChecksum, 0),
		ExistingVersionId: 0,
		NeedsSave:         true,
		ObjectIdentifier:  objIdentifier,
		PathInBag:         pathInBag,
		StorageOption:     "Standard",
		StorageRecords:    make([]*StorageRecord, 0),
	}
}

// Returns the file's GenericFile.Identifier.
func (f *IngestFile) Identifier() string {
	return fmt.Sprintf("%s/%s", f.ObjectIdentifier, f.PathInBag)
}

// Returns the type of bag file: payload_file, manifest, tag_manifest,
// or tag_file.
func (f *IngestFile) FileType() string {
	fileType := constants.FileTypePayload
	if strings.HasPrefix(f.PathInBag, "tagmanifest-") {
		fileType = constants.FileTypeTagManifest
	} else if strings.HasPrefix(f.PathInBag, "manifest-") {
		fileType = constants.FileTypeManifest
	} else if !strings.HasPrefix(f.PathInBag, "data/") {
		fileType = constants.FileTypeTag
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
