package service

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util"
	"strings"
	"time"
)

type IngestFile struct {
	Checksums            []*IngestChecksum `json:"checksums"`
	CopiedToStagingAt    time.Time         `json:"copied_to_staging_at,omitempty"`
	ErrorMessage         string            `json:"error_message,omitempty"`
	FileFormat           string            `json:"file_format,omitempty"`
	FileModified         time.Time         `json:"file_modified,omitempty"`
	Id                   int               `json:"id,omitempty"`
	InstitutionId        int               `json:"institution_id,omitempty"`
	IntellectualObjectId int               `json:"intellectual_object_id,omitempty"`
	NeedsSave            bool              `json:"needs_save"`
	ObjectIdentifier     string            `json:"object_identifier"`
	PathInBag            string            `json:"path_in_bag"`
	Size                 int64             `json:"size"`
	StorageOption        string            `json:"storage_option"`
	StorageRecords       []*StorageRecord  `json:"storage_records"`
	UUID                 string            `json:"uuid"`
}

func NewIngestFile(objIdentifier, pathInBag string) *IngestFile {
	return &IngestFile{
		Checksums:        make([]*IngestChecksum, 0),
		NeedsSave:        true,
		ObjectIdentifier: objIdentifier,
		PathInBag:        pathInBag,
		StorageOption:    "Standard",
		StorageRecords:   make([]*StorageRecord, 0),
	}
}

func IngestFileFromJson(jsonData string) (*IngestFile, error) {
	f := &IngestFile{}
	err := json.Unmarshal([]byte(jsonData), f)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (f *IngestFile) ToJson() (string, error) {
	bytes, err := json.Marshal(f)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
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
	} else if f.PathInBag == "fetch.txt" {
		fileType = constants.FileTypeFetchTxt
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

func (f *IngestFile) SetStorageRecord(record *StorageRecord) {
	updated := false
	for i, rec := range f.StorageRecords {
		if rec.URL == record.URL {
			f.StorageRecords[i] = record
			updated = true
		}
	}
	if updated == false {
		f.StorageRecords = append(f.StorageRecords, record)
	}
}

func (f *IngestFile) GetStorageRecord(url string) *StorageRecord {
	for _, rec := range f.StorageRecords {
		if rec.URL == url {
			return rec
		}
	}
	return nil
}

// File identifiers cannot contain control characters.
func (f *IngestFile) IdentifierIsLegal() (bool, error) {
	var err error
	identifier := f.Identifier()
	ok := !util.ContainsControlCharacter(identifier) &&
		!util.ContainsEscapedControl(identifier)
	if !ok {
		err = fmt.Errorf("File name '%s' contains one or more "+
			"illegal control characters", f.PathInBag)
	}
	return ok, err
}

// Manifest checksums are required for payload files, but not for
// tag files. Tag manifests also don't have recorded checksums anywhere.
// Tag manifests must include checksums for payload manifests. So:
//
// data/somefile.txt MUST have an entry in the payload manifest
// manifest-sha256.md MUST have an entry in each tag manifest
// custom-tag-file.txt MAY have an entry in each tag manifest (not MUST)
//
// https://tools.ietf.org/html/rfc8493#section-2.2.1
func (f *IngestFile) ManifestChecksumRequired(manifestName string) bool {
	required := true
	fileType := f.FileType()
	if strings.HasPrefix(manifestName, "manifest-") {
		// Payload files MUST be in manifest
		required = (fileType == constants.FileTypePayload)
	} else if strings.HasPrefix(manifestName, "tagmanifest-") {
		// Manifests MUST be in tag manifests, but tag files MAY
		// be excluded from tag manifests.
		required = (fileType == constants.FileTypeManifest)
	} else {
		// Panic because this is entirely in the developer's control.
		msg := fmt.Errorf("Unrecognized manifest type %s. "+
			"Name should start with 'manifest-' or 'tagmanifest-'",
			manifestName)
		panic(msg)
	}
	return required
}

// ChecksumsMatch returns true if this file's manifest checksum matches
// the checksum calculated at ingest. If the checksums don't match, this
// will return a specific error describing one of the following conditions:
//
// 1. Checksum was in manifest, but file was not in bag. This is a missing
// file error, and the bag is invalid.
//
// 2. The file was in the bag but not in the manifest. This is an illegal
// freeloader, and the bag is invalid.
//
// 3. The file was in the manifest and in the bag, but the checksums don't
// match.
func (f *IngestFile) ChecksumsMatch(manifestName string) (bool, error) {
	ok := true
	alg, err := util.AlgorithmFromManifestName(manifestName)
	if err != nil {
		msg := fmt.Sprintf("Unrecognized manifest name: %s", err.Error())
		panic(msg)
	}
	ingestChecksum := f.GetChecksum(constants.SourceIngest, alg)
	var manifestChecksum *IngestChecksum
	if util.LooksLikeTagManifest(manifestName) {
		manifestChecksum = f.GetChecksum(constants.SourceTagManifest, alg)
	} else {
		manifestChecksum = f.GetChecksum(constants.SourceManifest, alg)
	}

	manifestChecksumRequired := f.ManifestChecksumRequired(manifestName)

	if ingestChecksum == nil && manifestChecksum != nil {
		err = fmt.Errorf("File %s in %s is missing from bag",
			f.Identifier(), manifestName)
		ok = false
	}
	if manifestChecksum == nil && manifestChecksumRequired {
		err = fmt.Errorf("File %s is not in manifest %s",
			f.Identifier(), manifestName)
		ok = false
	}
	if ingestChecksum != nil && manifestChecksum != nil {
		if ingestChecksum.Digest != manifestChecksum.Digest {
			err = fmt.Errorf("File %s: ingest %s checksum %s "+
				"doesn't match manifest checksum %s",
				f.Identifier(), alg, ingestChecksum.Digest,
				manifestChecksum.Digest)
			ok = false
		}
	}
	return ok, err
}

// URI returns the URL of this file's first storage record.
// TODO: Fix this, because it doesn't map to Pharos' db structure.
// Pharos allows one URI per generic file, but it should allow
// multiple, as this does. Allowing multiple will better support
// the standard storage option, which includes two URLs (S3 VA
// and Glacier OR). The change will also allow us to offer mixed
// storage options, such as Glacier-OH + Wasabi-CA
//
// This needs to be fixed in Pharos.
// See https://trello.com/c/4Hx4KQDR
func (f *IngestFile) URI() string {
	uri := ""
	recs := f.StorageRecords
	if recs != nil && len(recs) > 0 && recs[0] != nil {
		uri = recs[0].URL
	}
	return uri
}

func (f *IngestFile) ToGenericFile() *registry.GenericFile {
	return &registry.GenericFile{
		FileFormat:                   f.FileFormat,
		FileModified:                 f.FileModified,
		Id:                           f.Id,
		Identifier:                   f.Identifier(),
		InstitutionId:                f.InstitutionId,
		IntellectualObjectId:         f.IntellectualObjectId,
		IntellectualObjectIdentifier: f.ObjectIdentifier,
		Size:                         f.Size,
		State:                        constants.StateActive,
		StorageOption:                f.StorageOption,
		URI:                          f.URI(),
	}
}
