package registry

import (
	"encoding/json"
	"strings"
	"time"
)

type GenericFile struct {
	Checksums                    []*Checksum    `json:"checksums,omitempty"`
	CreatedAt                    time.Time      `json:"created_at,omitempty"`
	FileFormat                   string         `json:"file_format,omitempty"`
	FileModified                 time.Time      `json:"file_modified,omitempty"`
	ID                           int            `json:"id,omitempty"`
	Identifier                   string         `json:"identifier,omitempty"`
	InstitutionID                int            `json:"institution_id,omitempty"`
	IntellectualObjectID         int            `json:"intellectual_object_id,omitempty"`
	IntellectualObjectIdentifier string         `json:"intellectual_object_identifier,omitempty"`
	LastFixityCheck              time.Time      `json:"last_fixity_check,omitempty"`
	PremisEvents                 []*PremisEvent `json:"premis_events,omitempty"`
	Size                         int64          `json:"size,omitempty"`
	State                        string         `json:"state,omitempty"`
	StorageOption                string         `json:"storage_option"`
	URI                          string         `json:"uri,omitempty"`
	UpdatedAt                    time.Time      `json:"updated_at,omitempty"`
}

func GenericFileFromJSON(jsonData []byte) (*GenericFile, error) {
	gf := &GenericFile{}
	err := json.Unmarshal(jsonData, gf)
	if err != nil {
		return nil, err
	}
	return gf, nil
}

func (gf *GenericFile) ToJSON() ([]byte, error) {
	bytes, err := json.Marshal(gf)
	if err != nil {
		return nil, err
	}
	return bytes, nil
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
	Checksums            []*Checksum    `json:"checksums_attributes,omitempty"`
	FileFormat           string         `json:"file_format,omitempty"`
	ID                   int            `json:"id,omitempty"`
	Identifier           string         `json:"identifier,omitempty"`
	InstitutionID        int            `json:"institution_id"`
	IntellectualObjectID int            `json:"intellectual_object_id"`
	PremisEvents         []*PremisEvent `json:"premis_events_attributes,omitempty"`
	Size                 int64          `json:"size,omitempty"`
	StorageOption        string         `json:"storage_option"`
	URI                  string         `json:"uri,omitempty"`
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
		URI:                  gf.URI,
	}
}
