package registry

import (
	"time"
)

// IntellectualObjectView is a read-only model that contains a flat
// representation of useful info about an IntellectualObject.
type IntellectualObjectView struct {
	ID                        int64     `json:"id"`
	Title                     string    `json:"title"`
	Description               string    `json:"description"`
	Identifier                string    `json:"identifier"`
	AltIdentifier             string    `json:"alt_identifier"`
	Access                    string    `json:"access"`
	BagName                   string    `json:"bag_name"`
	InstitutionID             int64     `json:"institution_id"`
	CreatedAt                 time.Time `json:"created_at"`
	UpdatedAt                 time.Time `json:"updated_at"`
	State                     string    `json:"state"`
	ETag                      string    `json:"etag" pg:"etag"`
	BagGroupIdentifier        string    `json:"bag_group_identifier"`
	StorageOption             string    `json:"storage_option"`
	BagItProfileIdentifier    string    `json:"bagit_profile_identifier" pg:"bagit_profile_identifier"`
	SourceOrganization        string    `json:"source_organization"`
	InternalSenderIdentifier  string    `json:"internal_sender_identifier"`
	InternalSenderDescription string    `json:"internal_sender_description"`
	InstitutionName           string    `json:"institution_name"`
	InstitutionIdentifier     string    `json:"institution_identifier"`
	InstitutionType           string    `json:"institution_type"`
	InstitutionParentID       int64     `json:"institution_parent_id"`
	FileCount                 int64     `json:"file_count"`
	Size                      int64     `json:"size"`
}
