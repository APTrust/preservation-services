package registry

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type IntellectualObject struct {
	Access                 string    `json:"access"`
	AltIdentifier          string    `json:"alt_identifier"`
	BagGroupIdentifier     string    `json:"bag_group_identifier"`
	BagItProfileIdentifier string    `json:"bagit_profile_identifier"`
	BagName                string    `json:"bag_name"`
	CreatedAt              time.Time `json:"created_at"`
	Description            string    `json:"description"`
	ETag                   string    `json:"etag"`
	FileCount              int64     `json:"file_count"`

	// TODO: Remove Pharos-legacy FileSize and use only Registry's Size
	FileSize                  int64  `json:"file_size"`
	Size                      int64  `json:"size"`
	ID                        int64  `json:"id"`
	Identifier                string `json:"identifier"`
	InternalSenderDescription string `json:"internal_sender_description"`
	InternalSenderIdentifier  string `json:"internal_sender_identifier"`

	// TODO: Delete Institution when we get rid of Pharos client.
	// Registry uses InstitutionIdentifier instead. This trips up
	// the registry client, so we're not serializing it for now.
	// Again, remove when Pharos client is gone.
	Institution           string    `json:"-"`
	InstitutionIdentifier string    `json:"institution_identifier"`
	InstitutionID         int64     `json:"institution_id"`
	SourceOrganization    string    `json:"source_organization"`
	State                 string    `json:"state"`
	StorageOption         string    `json:"storage_option"`
	Title                 string    `json:"title"`
	UpdatedAt             time.Time `json:"updated_at"`
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
	InstitutionID             int64  `json:"institution_id"`
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
