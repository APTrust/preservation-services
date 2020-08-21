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
	Checksums                    []*Checksum      `json:"checksums,omitempty"`
	CreatedAt                    time.Time        `json:"created_at,omitempty"`
	FileFormat                   string           `json:"file_format,omitempty"`
	FileModified                 time.Time        `json:"file_modified,omitempty"`
	ID                           int              `json:"id,omitempty"`
	Identifier                   string           `json:"identifier,omitempty"`
	InstitutionID                int              `json:"institution_id,omitempty"`
	IntellectualObjectID         int              `json:"intellectual_object_id,omitempty"`
	IntellectualObjectIdentifier string           `json:"intellectual_object_identifier,omitempty"`
	LastFixityCheck              time.Time        `json:"last_fixity_check,omitempty"`
	PremisEvents                 []*PremisEvent   `json:"premis_events,omitempty"`
	Size                         int64            `json:"size"`
	State                        string           `json:"state,omitempty"`
	StorageOption                string           `json:"storage_option"`
	StorageRecords               []*StorageRecord `json:"storage_records,omitempty"`
	URI                          string           `json:"uri,omitempty"`
	UpdatedAt                    time.Time        `json:"updated_at,omitempty"`
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
func (gf *GenericFile) PathInBag() string {
	return strings.Replace(gf.Identifier, gf.IntellectualObjectIdentifier+"/", "", 1)
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

// UUID returns this file's UUID. This is the identifier we use in
// preservation storage.
func (gf *GenericFile) UUID() string {
	parts := strings.Split(gf.URI, "/")
	return parts[len(parts)-1]
}

type GenericFileForPharos struct {
	Checksums            []*Checksum      `json:"checksums_attributes,omitempty"`
	FileFormat           string           `json:"file_format"`
	ID                   int              `json:"id,omitempty"`
	Identifier           string           `json:"identifier,omitempty"`
	InstitutionID        int              `json:"institution_id"`
	IntellectualObjectID int              `json:"intellectual_object_id"`
	PremisEvents         []*PremisEvent   `json:"premis_events_attributes,omitempty"`
	Size                 int64            `json:"size"`
	StorageOption        string           `json:"storage_option"`
	StorageRecords       []*StorageRecord `json:"storage_records_attributes,omitempty"`
	URI                  string           `json:"uri,omitempty"`
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
		URI:                  gf.URI,
	}
}
