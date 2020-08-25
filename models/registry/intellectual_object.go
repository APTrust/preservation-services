package registry

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type IntellectualObject struct {
	Access                    string    `json:"access,omitempty"`
	AltIdentifier             string    `json:"alt_identifier,omitempty"`
	BagGroupIdentifier        string    `json:"bag_group_identifier,omitempty"`
	BagItProfileIdentifier    string    `json:"bagit_profile_identifier,omitempty"`
	BagName                   string    `json:"bag_name,omitempty"`
	CreatedAt                 time.Time `json:"created_at,omitempty"`
	Description               string    `json:"description,omitempty"`
	ETag                      string    `json:"etag,omitempty"`
	FileCount                 int       `json:"file_count,omitempty"`
	FileSize                  int64     `json:"file_size,omitempty"`
	ID                        int       `json:"id,omitempty"`
	Identifier                string    `json:"identifier,omitempty"`
	InternalSenderDescription string    `json:"internal_sender_description,omitempty"`
	InternalSenderIdentifier  string    `json:"internal_sender_identifier,omitempty"`
	Institution               string    `json:"institution,omitempty"`
	InstitutionID             int       `json:"institution_id,omitempty"`
	SourceOrganization        string    `json:"source_organization,omitempty"`
	State                     string    `json:"state"`
	StorageOption             string    `json:"storage_option"`
	Title                     string    `json:"title,omitempty"`
	UpdatedAt                 time.Time `json:"updated_at,omitempty"`
}

func IntellectualObjectFromJSON(jsonData []byte) (*IntellectualObject, error) {
	obj := &IntellectualObject{}
	err := json.Unmarshal(jsonData, obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (obj *IntellectualObject) ToJSON() ([]byte, error) {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (obj *IntellectualObject) IdentifierMinusInstitution() (string, error) {
	parts := strings.SplitN(obj.Identifier, "/", 2)
	if len(parts) < 2 {
		return "", fmt.Errorf("Invalid identifier '%s': missing institution prefix", obj.Identifier)
	}
	return parts[1], nil
}

// JSON format for Pharos post/put is {"intellectual_object": <object>}
// Also note that we don't serialize fields that Pharos doesn't accept.
func (obj *IntellectualObject) SerializeForPharos() ([]byte, error) {
	dataStruct := make(map[string]*IntellectualObjectForPharos)
	dataStruct["intellectual_object"] = NewIntellectualObjectForPharos(obj)
	return json.Marshal(dataStruct)
}

type IntellectualObjectForPharos struct {
	Access                    string `json:"access"`
	AltIdentifier             string `json:"alt_identifier"`
	BagGroupIdentifier        string `json:"bag_group_identifier"`
	BagItProfileIdentifier    string `json:"bagit_profile_identifier"`
	BagName                   string `json:"bag_name"`
	Description               string `json:"description"`
	ETag                      string `json:"etag"`
	Identifier                string `json:"identifier"`
	InternalSenderDescription string `json:"internal_sender_description"`
	InternalSenderIdentifier  string `json:"internal_sender_identifier"`
	InstitutionID             int    `json:"institution_id"`
	SourceOrganization        string `json:"source_organization"`
	State                     string `json:"state"`
	StorageOption             string `json:"storage_option"`
	Title                     string `json:"title"`
}

func NewIntellectualObjectForPharos(obj *IntellectualObject) *IntellectualObjectForPharos {
	return &IntellectualObjectForPharos{
		Access:                    obj.Access,
		AltIdentifier:             obj.AltIdentifier,
		BagGroupIdentifier:        obj.BagGroupIdentifier,
		BagItProfileIdentifier:    obj.BagItProfileIdentifier,
		BagName:                   obj.BagName,
		Description:               obj.Description,
		ETag:                      obj.ETag,
		Identifier:                obj.Identifier,
		InternalSenderDescription: obj.InternalSenderDescription,
		InternalSenderIdentifier:  obj.InternalSenderIdentifier,
		InstitutionID:             obj.InstitutionID,
		SourceOrganization:        obj.SourceOrganization,
		State:                     obj.State,
		StorageOption:             obj.StorageOption,
		Title:                     obj.Title,
	}
}
