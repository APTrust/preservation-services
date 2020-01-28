package registry

import (
	"encoding/json"
	"time"
)

type IntellectualObject struct {
	Access                 string    `json:"access,omitempty"`
	AltIdentifier          string    `json:"alt_identifier,omitempty"`
	BagGroupIdentifier     string    `json:"bag_group_identifier,omitempty"`
	BagItProfileIdentifier string    `json:"bagit_profile_identifier,omitempty"`
	BagName                string    `json:"bag_name,omitempty"`
	CreatedAt              time.Time `json:"created_at,omitempty"`
	Description            string    `json:"description,omitempty"`
	ETag                   string    `json:"etag,omitempty"`
	FileCount              int       `json:"file_count,omitempty"`
	FileSize               int64     `json:"file_size,omitempty"`
	Id                     int       `json:"id,omitempty"`
	Identifier             string    `json:"identifier,omitempty"`
	Institution            string    `json:"institution,omitempty"`
	InstitutionId          int       `json:"institution_id,omitempty"`
	SourceOrganization     string    `json:"source_organization,omitempty"`
	State                  string    `json:"state"`
	StorageOption          string    `json:"storage_option"`
	Title                  string    `json:"title,omitempty"`
	UpdatedAt              time.Time `json:"updated_at,omitempty"`
}

func IntellectualObjectFromJson(jsonData string) (*IntellectualObject, error) {
	obj := &IntellectualObject{}
	err := json.Unmarshal([]byte(jsonData), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (obj *IntellectualObject) ToJson() (string, error) {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
