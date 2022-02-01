package registry

import (
	"time"
)

// GenericFileView is a read-only model that includes some additional
// info over the standard GenericFile.
type GenericFileView struct {
	ID                    int64     `json:"id" pg:"id"`
	FileFormat            string    `json:"file_format"`
	Size                  int64     `json:"size"`
	Identifier            string    `json:"identifier"`
	IntellectualObjectID  int64     `json:"intellection_object_id"`
	ObjectIdentifier      string    `json:"object_identifier"`
	Access                string    `json:"access"`
	State                 string    `json:"state"`
	LastFixityCheck       time.Time `json:"last_fixity_check"`
	InstitutionID         int64     `json:"institution_id"`
	InstitutionName       string    `json:"institution_name"`
	InstitutionIdentifier string    `json:"institution_identifier"`
	StorageOption         string    `json:"storage_option"`
	UUID                  string    `json:"uuid" pg:"uuid"`
	Md5                   string    `json:"md5"`
	Sha1                  string    `json:"sha1"`
	Sha256                string    `json:"sha256"`
	Sha512                string    `json:"sha512"`
	CreatedAt             time.Time `json:"created_at"`
	UpdatedAt             time.Time `json:"updated_at"`
}
