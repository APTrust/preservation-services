package service

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util"
	"github.com/minio/minio-go/v6"
)

type IngestFile struct {
	Checksums            []*IngestChecksum       `json:"checksums"`
	CopiedToStagingAt    time.Time               `json:"copied_to_staging_at,omitempty"`
	ErrorMessage         string                  `json:"error_message,omitempty"`
	FileFormat           string                  `json:"file_format,omitempty"`
	FormatIdentifiedBy   string                  `json:"format_identified_by,omitempty"`
	FormatIdentifiedAt   time.Time               `json:"format_identified_at,omitempty"`
	FormatMatchType      string                  `json:"format_match_type,omitempty"`
	FileModified         time.Time               `json:"file_modified,omitempty"`
	ID                   int                     `json:"id,omitempty"`
	InstitutionID        int                     `json:"institution_id,omitempty"`
	IntellectualObjectID int                     `json:"intellectual_object_id,omitempty"`
	NeedsSave            bool                    `json:"needs_save"`
	ObjectIdentifier     string                  `json:"object_identifier"`
	PathInBag            string                  `json:"path_in_bag"`
	PremisEvents         []*registry.PremisEvent `json:"premis_events,omitempty"`
	Size                 int64                   `json:"size"`
	StorageOption        string                  `json:"storage_option"`
	StorageRecords       []*StorageRecord        `json:"storage_records"`
	UUID                 string                  `json:"uuid"`
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

func IngestFileFromJSON(jsonData string) (*IngestFile, error) {
	f := &IngestFile{}
	err := json.Unmarshal([]byte(jsonData), f)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (f *IngestFile) ToJSON() (string, error) {
	bytes, err := json.Marshal(f)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// Identifier returns the file's GenericFile.Identifier.
func (f *IngestFile) Identifier() string {
	return fmt.Sprintf("%s/%s", f.ObjectIdentifier, f.PathInBag)
}

// Institution returns this file's institution identifier, which
// is typically a domain name.
func (f *IngestFile) Institution() string {
	return strings.Split(f.ObjectIdentifier, "/")[0]
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

// SetStorageRecord adds or updates a storage record.
func (f *IngestFile) SetStorageRecord(record *StorageRecord) {
	updated := false
	for i, rec := range f.StorageRecords {
		if rec.Provider == record.Provider && rec.Bucket == record.Bucket {
			f.StorageRecords[i] = record
			updated = true
		}
	}
	if updated == false {
		f.StorageRecords = append(f.StorageRecords, record)
	}
}

func (f *IngestFile) GetStorageRecord(provider, bucket string) *StorageRecord {
	for _, rec := range f.StorageRecords {
		if rec.Provider == provider && rec.Bucket == bucket {
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

// FidoSafeName returns the IngestFile's UUID + extension.
// E.g. "209b478a-95cd-4217-b0a3-c80e3e7a2f0e.pdf"
//
// If FIDO can't identify the file from the first 128k of
// data, it will identify it by file extension.
// Since FmtIdentifier will exec an external command,
// we want to be sure to pass a safe filename.
// The file UUID + extension allows FIDO to see the file
// extension, while ensuring that the file name is shell-safe.
// We DO NOT want to pass in some of the file names we get
// that contain backticks, curly braces, dollar signs, etc.
func (f *IngestFile) FidoSafeName() string {
	return f.UUID + path.Ext(f.PathInBag)
}

// GetPutOptions returns the metadata we'll need to store with a file
// in the staging bucket, and later in preservation storage. The metadata
// inclues the following:
//
// * institution - The identifier of the institution that owns the file.
//
// * bag - The name of the intellectual object to which the file belongs.
//
// * bagpath - The path of this file within the original bag. You can derive
//   the file's identifier by combining institution/bag/bagpath
//
// * md5 - The md5 digest of this file.
//
// * sha256 - The sha256 digest of this file.
//
func (f *IngestFile) GetPutOptions() (minio.PutObjectOptions, error) {
	emptyOpts := minio.PutObjectOptions{}
	md5 := f.GetChecksum(constants.SourceIngest, constants.AlgMd5)
	if md5 == nil {
		return emptyOpts, fmt.Errorf("%s has no ingest md5 checksum", f.Identifier())
	}
	sha256 := f.GetChecksum(constants.SourceIngest, constants.AlgSha256)
	if sha256 == nil {
		return emptyOpts, fmt.Errorf("%s has no ingest sha256 checksum", f.Identifier())
	}
	return minio.PutObjectOptions{
		UserMetadata: map[string]string{
			"institution": f.Institution(),
			"bag":         f.ObjectIdentifier,
			"bagpath":     f.PathInBag,
			"md5":         md5.Digest,
			"sha256":      sha256.Digest,
		},
		ContentType: f.FileFormat,
	}, nil
}

// HasPreservableName returns true if this file's name indicates it should
// be preserved. We preserve all files except: "bagit.txt", "fetch.txt",
// tag manifests and payload manifests.
func (f *IngestFile) HasPreservableName() bool {
	fileType := f.FileType()
	if f.PathInBag == "bagit.txt" ||
		fileType == constants.FileTypeFetchTxt ||
		fileType == constants.FileTypeManifest ||
		fileType == constants.FileTypeTagManifest {
		return false
	}
	return true
}

// NeedsSaveAt returns true if this file needs to be copied the specified
// bucket at the specified provider. This will return true if the file
// has a savable name and has no confirmed storage record at the specified
// provider + bucket.
//
// The ingest processes that manipulate this file are responsible for
// creating and updating this file's storage records. Also note that this
// will return true if you pass in bogus provider and bucket names,
// because the file likely has not been stored at those places.
// Therefore, it's the caller's responsibility to know, based on the file's
// StorageOption, whether the file actually *should* be stored at
// the provider + bucket.
func (f *IngestFile) NeedsSaveAt(provider, bucket string) bool {
	if f.HasPreservableName() == false {
		return false
	}
	storageRecord := f.GetStorageRecord(provider, bucket)
	return storageRecord == nil || storageRecord.StoredAt.IsZero()
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
		ID:                           f.ID,
		Identifier:                   f.Identifier(),
		InstitutionID:                f.InstitutionID,
		IntellectualObjectID:         f.IntellectualObjectID,
		IntellectualObjectIdentifier: f.ObjectIdentifier,
		Size:                         f.Size,
		State:                        constants.StateActive,
		StorageOption:                f.StorageOption,
		URI:                          f.URI(),
	}
}
