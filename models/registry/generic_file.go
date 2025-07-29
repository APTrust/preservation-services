package registry

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/util"
)

// GenericFile represents a Registry GenericFile object.
// Note that FileModified is currently not being stored in Registry.
type GenericFile struct {
	Checksums            []*Checksum      `json:"checksums"`
	CreatedAt            time.Time        `json:"created_at"`
	FileFormat           string           `json:"file_format"`
	FileModified         time.Time        `json:"file_modified"`
	ID                   int64            `json:"id"`
	Identifier           string           `json:"identifier"`
	InstitutionID        int64            `json:"institution_id"`
	IntellectualObjectID int64            `json:"intellectual_object_id"`
	LastFixityCheck      time.Time        `json:"last_fixity_check"`
	PremisEvents         []*PremisEvent   `json:"premis_events"`
	Size                 int64            `json:"size"`
	State                string           `json:"state"`
	StorageOption        string           `json:"storage_option"`
	StorageRecords       []*StorageRecord `json:"storage_records"`
	UUID                 string           `json:"uuid"`
	UpdatedAt            time.Time        `json:"updated_at"`
	ModTime              time.Time        `json:"mtime"`
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

// IntellectualObjectIdentifier returns this file's intellectual
// object identifier, or an error if it can't determine the object
// identifier.
func (gf *GenericFile) IntellectualObjectIdentifier() (string, error) {
	parts := strings.Split(gf.Identifier, "/")
	if len(parts) > 1 {
		ident := strings.Join(parts[0:2], "/")
		return ident, nil
	}
	return "", fmt.Errorf("invalid identifier: %s", gf.Identifier)
}

// PathInBag returns the path of this file within the original bag.
// Example: If Identifier is "test.edu/bag/data/file.txt", this will
// return "data/file.txt"
func (gf *GenericFile) PathInBag() (string, error) {
	parts := strings.Split(gf.Identifier, "/")
	if len(parts) > 2 {
		return strings.Join(parts[2:], "/"), nil
	}
	return "", fmt.Errorf("invalid identifier: %s", gf.Identifier)
}

// PathMinusInstitution is the object name plus PathInBag(). In other words,
// the full Identifier minus the leading institution name.
// Example: If Identifier is "test.edu/bag/data/file.txt", this will
// return "bag/data/file.txt"
func (gf *GenericFile) PathMinusInstitution() (string, error) {
	parts := strings.Split(gf.Identifier, "/")
	if len(parts) > 1 {
		return strings.Join(parts[1:], "/"), nil
	}
	return "", fmt.Errorf("invalid identifier: %s", gf.Identifier)
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
	pathInBag, _ := gf.PathInBag()
	return !strings.HasPrefix(pathInBag, "data") && !util.LooksLikeManifest(pathInBag) && !util.LooksLikeTagManifest(pathInBag)
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
