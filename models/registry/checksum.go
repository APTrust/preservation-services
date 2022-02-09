package registry

import (
	"encoding/json"
	"time"
)

type Checksum struct {
	// ID is the checksum's unique identifier in the database.
	ID int64 `json:"id,omitempty"`

	// Algorithm is the digest algorithm. E.g. "md5", "sha256", etc.
	// See constants.DigestAlgorithms.
	Algorithm string `json:"algorithm"`

	// Digest is the checksum digest.
	Digest string `json:"digest"`

	// DateTime is the timestamp of when this digest was calculated.
	// Use this instead of CreatedAt and UpdatedAt.
	DateTime time.Time `json:"datetime"`

	// GenericFileID is the ID of the GenericFile to which this
	// checksum belongs.
	GenericFileID int64 `json:"generic_file_id"`

	// CreatedAt is a legacy timestamp from the old Rails app.
	// Ignore this and use DateTime instead.
	CreatedAt time.Time `json:"created_at,omitempty"`

	// UpdatedAt is a legacy timestamp from the old Rails app.
	// Ignore this and use DateTime instead. Also note that
	// checksums are never updated.
	UpdatedAt time.Time `json:"updated_at,omitempty"`

	// GenericFileIdentifier is the identifier of the GenericFile to
	// which this checksum belongs. This is a read-only field from
	// the Registry's ChecksumView object. No need to fill this on
	// POST or PUT requests. Registry will ignore it.
	GenericFileIdentifier string `json:"generic_file_identifier"`

	// IntellectualObjectID is the ID of the GenericFile's
	// parent object. This is a read-only field from
	// the Registry's ChecksumView object. No need to fill this on
	// POST or PUT requests. Registry will ignore it.
	IntellectualObjectID int64 `json:"intellectual_object_id"`

	// InstitutionID is the ID of the institutiond to
	// which this checksum belongs. This is a read-only field from
	// the Registry's ChecksumView object. No need to fill this on
	// POST or PUT requests. Registry will ignore it.
	InstitutionID int64 `json:"institution_id"`
}

func ChecksumFromJSON(jsonData []byte) (*Checksum, error) {
	c := &Checksum{}
	err := json.Unmarshal(jsonData, c)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Checksum) ToJSON() ([]byte, error) {
	bytes, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (c *Checksum) SerializeForPharos() ([]byte, error) {
	dataStruct := make(map[string]*Checksum)
	dataStruct["checksum"] = c
	return json.Marshal(dataStruct)
}
