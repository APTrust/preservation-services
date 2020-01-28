package registry

import (
	"encoding/json"
	"time"
)

type GenericFile struct {
	CreatedAt                    time.Time `json:"created_at,omitempty"`
	FileCreated                  time.Time `json:"file_created,omitempty"`
	FileFormat                   string    `json:"file_format,omitempty"`
	FileModified                 time.Time `json:"file_modified,omitempty"`
	Id                           int       `json:"id,omitempty"`
	Identifier                   string    `json:"identifier,omitempty"`
	IntellectualObjectId         int       `json:"intellectual_object_id,omitempty"`
	IntellectualObjectIdentifier string    `json:"intellectual_object_identifier,omitempty"`
	LastFixityCheck              time.Time `json:"last_fixity_check,omitempty"`
	Size                         int64     `json:"size,omitempty"`
	State                        string    `json:"state,omitempty"`
	StorageOption                string    `json:"storage_option"`
	URI                          string    `json:"uri,omitempty"`
	UpdatedAt                    time.Time `json:"updated_at,omitempty"`
}

func GenericFileFromJson(jsonData string) (*GenericFile, error) {
	gf := &GenericFile{}
	err := json.Unmarshal([]byte(jsonData), gf)
	if err != nil {
		return nil, err
	}
	return gf, nil
}

func (gf *GenericFile) ToJson() (string, error) {
	bytes, err := json.Marshal(gf)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
