package registry

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/util"
)

// GenericFile represents a Pharos GenericFile object.
// Note that FileModified is currently not being stored in Pharos.
type GenericFile struct {
	Checksums            []*Checksum `json:"checksums,omitempty"`
	CreatedAt            time.Time   `json:"created_at,omitempty"`
	FileFormat           string      `json:"file_format,omitempty"`
	FileModified         time.Time   `json:"file_modified,omitempty"`
	ID                   int64       `json:"id,omitempty"`
	Identifier           string      `json:"identifier,omitempty"`
	InstitutionID        int64       `json:"institution_id,omitempty"`
	IntellectualObjectID int64       `json:"intellectual_object_id,omitempty"`

	// TODO: Change json to object_identifier to match Registry
	// Also note that this is read-only.
	IntellectualObjectIdentifier string `json:"intellectual_object_identifier,omitempty"`

	LastFixityCheck time.Time        `json:"last_fixity_check,omitempty"`
	PremisEvents    []*PremisEvent   `json:"premis_events,omitempty"`
	Size            int64            `json:"size"`
	State           string           `json:"state,omitempty"`
	StorageOption   string           `json:"storage_option"`
	StorageRecords  []*StorageRecord `json:"storage_records,omitempty"`
	UUID            string           `json:"uuid,omitempty"`

	// Md5 is read-only, from Registry's GenericFileView
	Md5 string `json:"md5"`
	// Sha1 is read-only, from Registry's GenericFileView
	Sha1 string `json:"sha1"`
	// Sha256 is read-only, from Registry's GenericFileView
	Sha256 string `json:"sha256"`
	// Sha512 is read-only, from Registry's GenericFileView
	Sha512 string `json:"sha512"`

	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// GenericFileFromJSON converts a JSON representation of a GenericFile
// to a GenericFile object.
func GenericFileFromJSON(jsonData []byte) (*GenericFile, error) {
	gf := &GenericFile{}
	err := json.Unmarshal(jsonData, gf)
	if err != nil {
		return nil, err
	}
	return gf, nil
}

// ToJSON returns a JSON representation of this object.
func (gf *GenericFile) ToJSON() ([]byte, error) {
	bytes, err := json.Marshal(gf)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// PathInBag returns the path of this file within the original bag.
// Example: If Identifier is "test.edu/bag/data/file.txt", this will
// return "data/file.txt"
func (gf *GenericFile) PathInBag() string {
	return strings.Replace(gf.Identifier, gf.IntellectualObjectIdentifier+"/", "", 1)
}

// PathMinusInstitution is the object name plus PathInBag(). In other words,
// the full Identifier minus the leading institution name.
// Example: If Identifier is "test.edu/bag/data/file.txt", this will
// return "bag/data/file.txt"
func (gf *GenericFile) PathMinusInstitution() string {
	return strings.Replace(gf.Identifier, gf.InstitutionIdentifier()+"/", "", 1)
}

// InstitutionIdentifier returns the Instition Identifier from the beginning
// of the GenericFile Identifier. For example, if GenericFile Identifier
// is is "test.edu/bag/data/file.txt", this will return "test.edu"
func (gf *GenericFile) InstitutionIdentifier() string {
	return strings.SplitN(gf.Identifier, "/", 2)[0]
}

// IsTagFile returns true if this file's original path in the bag
// was not in the data (payload) directory, and the
func (gf *GenericFile) IsTagFile() bool {
	pathInBag := gf.PathInBag()
	return !strings.HasPrefix(pathInBag, "data") && !util.LooksLikeManifest(pathInBag) && !util.LooksLikeTagManifest(pathInBag)
}

// JSON format for Pharos post/put is {"generic_file": <object>}
// Also note that we don't serialize fields that Pharos doesn't accept.
func (gf *GenericFile) SerializeForPharos() ([]byte, error) {
	dataStruct := make(map[string]*GenericFileForPharos)
	dataStruct["generic_file"] = NewGenericFileForPharos(gf)
	return json.Marshal(dataStruct)
}

// GetLatestChecksum returns the most recent checksum digest for the given
// algorithm for this file.
func (gf *GenericFile) GetLatestChecksum(algorithm string) *Checksum {
	var checksum *Checksum
	latest := time.Time{}
	for _, cs := range gf.Checksums {
		if cs != nil && cs.Algorithm == algorithm && cs.DateTime.After(latest) {
			checksum = cs
			latest = cs.DateTime
		}
	}
	return checksum
}

type GenericFileForPharos struct {
	Checksums            []*Checksum      `json:"checksums_attributes,omitempty"`
	FileFormat           string           `json:"file_format"`
	ID                   int64            `json:"id,omitempty"`
	Identifier           string           `json:"identifier,omitempty"`
	InstitutionID        int64            `json:"institution_id"`
	IntellectualObjectID int64            `json:"intellectual_object_id"`
	PremisEvents         []*PremisEvent   `json:"premis_events_attributes,omitempty"`
	Size                 int64            `json:"size"`
	StorageOption        string           `json:"storage_option"`
	StorageRecords       []*StorageRecord `json:"storage_records_attributes,omitempty"`
	UUID                 string           `json:"uuid,omitempty"`
}

func NewGenericFileForPharos(gf *GenericFile) *GenericFileForPharos {
	return &GenericFileForPharos{
		Checksums:            gf.Checksums,
		FileFormat:           gf.FileFormat,
		ID:                   gf.ID,
		Identifier:           gf.Identifier,
		InstitutionID:        gf.InstitutionID,
		IntellectualObjectID: gf.IntellectualObjectID,
		PremisEvents:         gf.PremisEvents,
		Size:                 gf.Size,
		StorageOption:        gf.StorageOption,
		StorageRecords:       gf.StorageRecords,
		UUID:                 gf.UUID,
	}
}
