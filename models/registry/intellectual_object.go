package registry

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type IntellectualObject struct {
	Access                    string    `json:"access"`
	AltIdentifier             string    `json:"alt_identifier"`
	BagGroupIdentifier        string    `json:"bag_group_identifier"`
	BagItProfileIdentifier    string    `json:"bagit_profile_identifier"`
	BagName                   string    `json:"bag_name"`
	CreatedAt                 time.Time `json:"created_at"`
	Description               string    `json:"description"`
	ETag                      string    `json:"etag"`
	FileCount                 int64     `json:"file_count"`
	Size                      int64     `json:"size"`
	ID                        int64     `json:"id"`
	Identifier                string    `json:"identifier"`
	InternalSenderDescription string    `json:"internal_sender_description"`
	InternalSenderIdentifier  string    `json:"internal_sender_identifier"`
	InstitutionIdentifier     string    `json:"institution_identifier"`
	InstitutionID             int64     `json:"institution_id"`
	SourceOrganization        string    `json:"source_organization"`
	State                     string    `json:"state"`
	StorageOption             string    `json:"storage_option"`
	Title                     string    `json:"title"`
	UpdatedAt                 time.Time `json:"updated_at"`
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
