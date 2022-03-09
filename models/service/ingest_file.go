package service

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
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
	ID                   int64                   `json:"id,omitempty"`
	InstitutionID        int64                   `json:"institution_id,omitempty"`
	IntellectualObjectID int64                   `json:"intellectual_object_id,omitempty"`
	IsReingest           bool                    `json:"is_reingest"`
	NeedsSave            bool                    `json:"needs_save"`
	ObjectIdentifier     string                  `json:"object_identifier"`
	PathInBag            string                  `json:"path_in_bag"`
	PremisEvents         []*registry.PremisEvent `json:"premis_events,omitempty"`
	RegistryURLs         []string                `json:"registry_urls"`
	SavedToRegistryAt    time.Time               `json:"saved_to_registry_at,omitempty"`
	Size                 int64                   `json:"size"`

	// StorageOption comes from the parent object, which gets from the
	// Storage-Option tag or APTrust-Storage-Option tag in the bag. This
	// property is set by the recorder, just before IngestFile is converted
	// to GenericFile to be sent to the Registry.
	//
	// We wait to set this because 1) we don't know the requested storage
	// option until we've parsed the bag's tag files, which often happens
	// after we've created the IngestFile; and 2) for reingests, we may have
	// to force the parent object's StorageOption to match that of the
	// already-ingested version. (That prevents us having divergent versions
	// in different preservation buckets. This is publicly documented in the
	// "Note" at https://aptrust.github.io/userguide/bagging/#allowed-storage-option-values)
	StorageOption  string           `json:"storage_option"`
	StorageRecords []*StorageRecord `json:"storage_records"`
	UUID           string           `json:"uuid"`
}

func NewIngestFile(objIdentifier, pathInBag string) *IngestFile {
	return &IngestFile{
		Checksums:        make([]*IngestChecksum, 0),
		NeedsSave:        true,
		ObjectIdentifier: objIdentifier,
		PathInBag:        pathInBag,
		RegistryURLs:     make([]string, 0),
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
// bucket at the specified provider. This will return true if 1) the file
// has a savable name and 2) the is flagged as needing to be saved, and 3)
// the file has no confirmed storage record at the specified provider + bucket.
//
// The ReingestManager will mark an IngestFile as NeedsSave = false
// if the file's checksums have not changed since the last ingest. This is a
// fairly common case, and will cause NeedsSaveAt to return false.
//
// The ingest processes that manipulate this file are responsible for
// creating and updating this file's storage records. Also note that this
// will return true if you pass in bogus provider and bucket names,
// because the file likely has not been stored at those places.
// Therefore, it's the caller's responsibility to know, based on the file's
// StorageOption, whether the file actually *should* be stored at
// the provider + bucket.
func (f *IngestFile) NeedsSaveAt(provider, bucket string) bool {
	if f.HasPreservableName() == false || f.NeedsSave == false {
		return false
	}
	storageRecord := f.GetStorageRecord(provider, bucket)
	return storageRecord == nil || storageRecord.StoredAt.IsZero()
}

// HasRegistryURL returns true if this IngestFile's
// RegistryURLs list contains the specified URL.
func (f *IngestFile) HasRegistryURL(url string) bool {
	return util.StringListContains(f.RegistryURLs, url)
}

// GetIngestEvents returns this files's list of ingest PremisEvents.
// It generates the list if the list does not already exist.
//
// Note that this list should be generated only once, and the events
// should be preserved in Redis so that if any part of registry data
// recording process fails, we can retry and know that we are not
// creating new PremisEvents in Registry. When Registry sees these event
// UUIDs already exist, it will not create duplicate entries. If we
// don't persist events with their UUIDs in Redis intermediate storage,
// we will be sending new events with new UUIDs each time we retry
// the ingest recording process, and we'll have lots of duplicate
// events in our registry.
func (f *IngestFile) GetIngestEvents() ([]*registry.PremisEvent, error) {
	if f.PremisEvents == nil {
		f.PremisEvents = make([]*registry.PremisEvent, 0)
	}
	var err error
	if len(f.PremisEvents) == 0 && f.NeedsSave {
		err = f.initIngestEvents()
	}
	return f.PremisEvents, err
}

func (f *IngestFile) initIngestEvents() error {
	ingestEvent, err := f.NewFileIngestEvent()
	if err != nil {
		return err
	}

	var fixityCheckEvents = make([]*registry.PremisEvent, 0)
	for _, cs := range f.Checksums {
		if cs.Source == constants.SourceManifest {
			fixityCheckEvents = append(fixityCheckEvents, f.NewFileFixityCheckEvent(cs))
		}
	}

	var digestEvents = make([]*registry.PremisEvent, 0)
	for _, cs := range f.Checksums {
		if cs.Source == constants.SourceIngest {
			digestEvents = append(digestEvents, f.NewFileDigestEvent(cs))
		}
	}

	var idEvent *registry.PremisEvent
	if f.IsReingest == false {
		idEvent, err = f.NewFileIdentifierEvent(f.Identifier(), constants.IdTypeBagAndPath)
		if err != nil {
			return err
		}
	}

	var urlEvent *registry.PremisEvent
	if f.IsReingest == false {
		urlEvent, err = f.NewFileIdentifierEvent(f.URI(), constants.IdTypeStorageURL)
		if err != nil {
			return err
		}
	}

	var replicationEvent *registry.PremisEvent
	if f.StorageRecords != nil && len(f.StorageRecords) > 1 {
		replicationEvent, err = f.NewFileReplicationEvent(f.StorageRecords[1])
		if err != nil {
			return err
		}
	}

	// Add events only after we know we've created them all successfully.
	// This prevents us having a partial event list. It's all or nothing.
	f.PremisEvents = append(f.PremisEvents, fixityCheckEvents...)
	f.PremisEvents = append(f.PremisEvents, digestEvents...)
	f.PremisEvents = append(f.PremisEvents, ingestEvent)
	if replicationEvent != nil {
		f.PremisEvents = append(f.PremisEvents, replicationEvent)
	}
	if idEvent != nil {
		f.PremisEvents = append(f.PremisEvents, idEvent)
	}
	if urlEvent != nil {
		f.PremisEvents = append(f.PremisEvents, urlEvent)
	}

	return nil
}

//
// TODO: Review this. Fix or remove.
//
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

// FindEvent returns the PremisEvent whose identifier matches the
// specified UUID, or nil.
func (f *IngestFile) FindEvent(eventUUID string) *registry.PremisEvent {
	for _, event := range f.PremisEvents {
		if event.Identifier == eventUUID {
			return event
		}
	}
	return nil
}

func (f *IngestFile) ToGenericFile() (*registry.GenericFile, error) {
	ingestEvents, err := f.GetIngestEvents()
	if err != nil {
		return nil, err
	}
	checksums := make([]*registry.Checksum, 0)
	for _, cs := range f.Checksums {
		if cs.Source == constants.SourceIngest {
			checksums = append(checksums, cs.ToRegistryChecksum(f.ID))
		}
	}
	storageRecords := make([]*registry.StorageRecord, 0)
	for _, r := range f.StorageRecords {
		// Tell Registry the file is stored at this URL only if
		// Registry doesn't already have a record of it.
		if !f.HasRegistryURL(r.URL) {
			storageRecords = append(storageRecords, &registry.StorageRecord{
				URL: r.URL,
			})
		}
	}
	var lastFixityCheck time.Time
	for _, event := range ingestEvents {
		if event.EventType == constants.EventDigestCalculation {
			lastFixityCheck = event.DateTime
		}
	}
	if lastFixityCheck.IsZero() {
		return nil, fmt.Errorf("cannot calculate last fixity check date from digest calculation event")
	}
	return &registry.GenericFile{
		Checksums:            checksums,
		FileFormat:           f.FileFormat,
		FileModified:         f.FileModified,
		ID:                   f.ID,
		Identifier:           f.Identifier(),
		InstitutionID:        f.InstitutionID,
		IntellectualObjectID: f.IntellectualObjectID,
		LastFixityCheck:      lastFixityCheck,
		PremisEvents:         ingestEvents,
		Size:                 f.Size,
		State:                constants.StateActive,
		StorageOption:        f.StorageOption,
		StorageRecords:       storageRecords,
		UUID:                 f.UUID,
	}, nil
}

// NewFileIngestEvent returns a PremisEvent describing a file ingest.
// Param storedAt should come from the IngestFile's primary StorageRecord.
// Param md5Digest should come from the IngestFiles md5 Checksum record.
// Param _uuid should come from IngestFile.UUID.
func (f *IngestFile) NewFileIngestEvent() (*registry.PremisEvent, error) {
	var firstStorageRecord *StorageRecord
	if f.StorageRecords != nil && len(f.StorageRecords) > 0 {
		firstStorageRecord = f.StorageRecords[0]
	}
	if firstStorageRecord == nil {
		return nil, fmt.Errorf("This file has no StorageRecords")
	}
	md5Checksum := f.GetChecksum(constants.SourceIngest, constants.AlgMd5)
	if md5Checksum == nil {
		return nil, fmt.Errorf("This file has no md5 checksum")
	}
	if firstStorageRecord.VerifiedAt.IsZero() {
		return nil, fmt.Errorf("Storage record has not been verified.")
	}
	eventId := uuid.New()
	timestamp := time.Now().UTC()
	return &registry.PremisEvent{
		Identifier:                   eventId.String(),
		EventType:                    constants.EventIngestion,
		DateTime:                     firstStorageRecord.StoredAt,
		Detail:                       fmt.Sprintf("Completed copy to preservation storage (%s)", f.UUID),
		Outcome:                      constants.StatusSuccess,
		OutcomeDetail:                fmt.Sprintf("md5:%s", md5Checksum.Digest),
		Object:                       "preservation-services + Minio S3 client",
		Agent:                        constants.S3ClientName,
		OutcomeInformation:           "Put using md5 checksum",
		IntellectualObjectIdentifier: f.ObjectIdentifier,
		GenericFileIdentifier:        f.Identifier(),
		InstitutionID:                f.InstitutionID,
		IntellectualObjectID:         f.IntellectualObjectID,
		CreatedAt:                    timestamp,
		UpdatedAt:                    timestamp,
	}, nil
}

// NewFileDigestEvent returns a PremisEvent describing the outcome of a
// fixity check. The check may occur at ingest or on a specified schedule against
// a file in preservation storage.
//
// If this event is being generated on ingest, all params should come from
// the IngestFile's Checksum record. When this event is generated by a scheduled
// fixity check, the params will come from the outcome of the check.
func (f *IngestFile) NewFileFixityCheckEvent(manifestChecksum *IngestChecksum) *registry.PremisEvent {
	eventId := uuid.New()
	timestamp := time.Now().UTC()
	props := getFixityProps(manifestChecksum.Algorithm, true)
	return &registry.PremisEvent{
		Identifier:                   eventId.String(),
		EventType:                    constants.EventFixityCheck,
		DateTime:                     manifestChecksum.DateTime,
		Detail:                       "Fixity check against registered hash",
		Outcome:                      props["outcome"],
		OutcomeDetail:                fmt.Sprintf("%s:%s", manifestChecksum.Algorithm, manifestChecksum.Digest),
		Object:                       props["object"],
		Agent:                        props["agent"],
		OutcomeInformation:           props["outcomeInformation"],
		IntellectualObjectIdentifier: f.ObjectIdentifier,
		GenericFileIdentifier:        f.Identifier(),
		InstitutionID:                f.InstitutionID,
		IntellectualObjectID:         f.IntellectualObjectID,
		CreatedAt:                    timestamp,
		UpdatedAt:                    timestamp,
	}
}

// NewFileDigestEvent returns a PremisEvent saying that we calculated a new
// checksum digest on this file during ingest.
func (f *IngestFile) NewFileDigestEvent(ingestChecksum *IngestChecksum) *registry.PremisEvent {
	eventId := uuid.New()
	timestamp := time.Now().UTC()
	props := getFixityProps(ingestChecksum.Algorithm, true)
	return &registry.PremisEvent{
		Identifier:                   eventId.String(),
		EventType:                    constants.EventDigestCalculation,
		DateTime:                     ingestChecksum.DateTime,
		Detail:                       "Calculated fixity value",
		Outcome:                      props["outcome"],
		OutcomeDetail:                fmt.Sprintf("%s:%s", ingestChecksum.Algorithm, ingestChecksum.Digest),
		Object:                       props["object"],
		Agent:                        props["agent"],
		OutcomeInformation:           "Calculated fixity value",
		IntellectualObjectIdentifier: f.ObjectIdentifier,
		GenericFileIdentifier:        f.Identifier(),
		InstitutionID:                f.InstitutionID,
		IntellectualObjectID:         f.IntellectualObjectID,
		CreatedAt:                    timestamp,
		UpdatedAt:                    timestamp,
	}
}

// NewFileIdentifierEvent returns a PremisEvent describing the identifier
// that was assigned to a file on ingest.
func (f *IngestFile) NewFileIdentifierEvent(identifier, identifierType string) (*registry.PremisEvent, error) {
	if identifier == "" {
		return nil, fmt.Errorf("Param identifier cannot be empty.")
	}
	eventId := uuid.New()
	timestamp := time.Now().UTC()
	object := "APTrust exchange/ingest processor"
	agent := "https://github.com/APTrust/preservation-services"
	detail := "Assigned new institution.bag/path identifier"
	if identifierType == constants.IdTypeStorageURL {
		object = "Go uuid library + Minio S3 library"
		agent = "http://github.com/google/uuid"
		detail = "Assigned new storage URL identifier"
	}
	return &registry.PremisEvent{
		Identifier:                   eventId.String(),
		EventType:                    constants.EventIdentifierAssignment,
		DateTime:                     time.Now().UTC(),
		Detail:                       detail,
		Outcome:                      string(constants.StatusSuccess),
		OutcomeDetail:                identifier,
		Object:                       object,
		Agent:                        agent,
		OutcomeInformation:           fmt.Sprintf("Assigned %s identifier", identifierType),
		IntellectualObjectIdentifier: f.ObjectIdentifier,
		GenericFileIdentifier:        f.Identifier(),
		InstitutionID:                f.InstitutionID,
		IntellectualObjectID:         f.IntellectualObjectID,
		CreatedAt:                    timestamp,
		UpdatedAt:                    timestamp,
	}, nil
}

// NewFileReplicationEvent returns a PremisEvent describing when a file
// was copied to replication storage. Params should come from the IngestFile's
// StorageRecord that describes where and when the replication copy was stored.
func (f *IngestFile) NewFileReplicationEvent(replicationRecord *StorageRecord) (*registry.PremisEvent, error) {
	if replicationRecord.StoredAt.IsZero() {
		return nil, fmt.Errorf("Replication record StoredAt cannot be empty")
	}
	if replicationRecord.VerifiedAt.IsZero() {
		return nil, fmt.Errorf("Replication record VerifiedAt cannot be empty")
	}
	eventId := uuid.New()
	timestamp := time.Now().UTC()
	return &registry.PremisEvent{
		Identifier:                   eventId.String(),
		EventType:                    constants.EventReplication,
		DateTime:                     replicationRecord.StoredAt,
		Detail:                       "Copied to replication storage and assigned replication URL identifier",
		Outcome:                      constants.StatusSuccess,
		OutcomeDetail:                replicationRecord.URL,
		Object:                       "Go uuid library + Minio S3 library",
		Agent:                        "http://github.com/google/uuid",
		OutcomeInformation:           "Replicated to secondary storage",
		IntellectualObjectIdentifier: f.ObjectIdentifier,
		GenericFileIdentifier:        f.Identifier(),
		InstitutionID:                f.InstitutionID,
		IntellectualObjectID:         f.IntellectualObjectID,
		CreatedAt:                    timestamp,
		UpdatedAt:                    timestamp,
	}, nil
}

func getFixityProps(fixityAlg string, fixityMatched bool) map[string]string {
	details := make(map[string]string)
	details["object"] = "Go language crypto/md5"
	details["agent"] = "http://golang.org/pkg/crypto/md5/"
	details["outcomeInformation"] = "Fixity matches"
	details["outcome"] = string(constants.StatusSuccess)
	if fixityAlg == constants.AlgSha256 {
		details["object"] = "Go language crypto/sha256"
		details["agent"] = "http://golang.org/pkg/crypto/sha256/"
	}
	if fixityMatched == false {
		details["outcome"] = string(constants.StatusFailed)
		details["outcomeInformation"] = "Fixity did not match"
	}
	return details
}
