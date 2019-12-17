package ingest

import (
	"time"
)

type IngestFile struct {
	Checksums                    []*IngestChecksum `json:"checksums"`
	CreatedAt                    time.Time         `json:"created_at,omitempty"`
	ErrorMessage                 string            `json:"ingesterror_message,omitempty"`
	FileCreated                  time.Time         `json:"file_created,omitempty"`
	FileFormat                   string            `json:"file_format,omitempty"`
	FileGid                      int               `json:"ingest_file_gid,omitempty"`
	FileGname                    string            `json:"ingest_file_gname,omitempty"`
	FileMode                     int64             `json:"ingest_file_mode,omitempty"`
	FileModified                 time.Time         `json:"file_modified,omitempty"`
	FileType                     string            `json:"ingest_file_type,omitempty"`
	FileUid                      int               `json:"ingest_file_uid,omitempty"`
	FileUname                    string            `json:"ingest_file_uname,omitempty"`
	Id                           int               `json:"id,omitempty"`
	Identifier                   string            `json:"identifier,omitempty"`
	IntellectualObjectId         int               `json:"intellectual_object_id,omitempty"`
	IntellectualObjectIdentifier string            `json:"intellectual_object_identifier,omitempty"`
	NeedsSave                    bool              `json:"ingest_needs_save,omitempty"`
	PreviousVersionExists        bool              `json:"ingest_previous_version_exists,omitempty"`
	Size                         int64             `json:"size,omitempty"`
	State                        string            `json:"state,omitempty"`
	StorageOption                string            `json:"storage_option"`
	StorageRecords               []*StorageRecord  `json:"storage_records"`
	URI                          string            `json:"uri,omitempty"`
	UUID                         string            `json:"ingest_uuid,omitempty"`
	UUIDGeneratedAt              time.Time         `json:"ingest_uuid_generated_at,omitempty"`
	UpdatedAt                    time.Time         `json:"updated_at,omitempty"`
}
