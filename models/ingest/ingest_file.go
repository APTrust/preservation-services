package ingest

import (
	"time"
)

type IngestFile struct {
	Checksums             []*IngestChecksum `json:"checksums"`
	CreatedAt             time.Time         `json:"created_at,omitempty"`
	ErrorMessage          string            `json:"ingesterror_message,omitempty"`
	FileCreated           time.Time         `json:"file_created,omitempty"`
	Format                string            `json:"file_format,omitempty"`
	Gid                   int               `json:"ingest_file_gid,omitempty"`
	Gname                 string            `json:"ingest_file_gname,omitempty"`
	Id                    int               `json:"id,omitempty"`
	Identifier            string            `json:"identifier,omitempty"`
	Mode                  int64             `json:"ingest_file_mode,omitempty"`
	Modified              time.Time         `json:"file_modified,omitempty"`
	NeedsSave             bool              `json:"ingest_needs_save,omitempty"`
	PreviousVersionExists bool              `json:"ingest_previous_version_exists,omitempty"`
	Size                  int64             `json:"size,omitempty"`
	State                 string            `json:"state,omitempty"`
	StorageOption         string            `json:"storage_option"`
	StorageRecords        []*StorageRecord  `json:"storage_records"`
	Type                  string            `json:"ingest_file_type,omitempty"`
	URI                   string            `json:"uri,omitempty"`
	UUID                  string            `json:"ingest_uuid,omitempty"`
	UUIDGeneratedAt       time.Time         `json:"ingest_uuid_generated_at,omitempty"`
	Uid                   int               `json:"ingest_file_uid,omitempty"`
	Uname                 string            `json:"ingest_file_uname,omitempty"`
	UpdatedAt             time.Time         `json:"updated_at,omitempty"`
}

func NewIngestFile(identifier string) *IngestFile {
	return &IngestFile{
		Checksums:             make([]*IngestChecksum, 0),
		Identifier:            identifier,
		NeedsSave:             true,
		PreviousVersionExists: false,
		State:                 "A",
		StorageOption:         "Standard",
		StorageRecords:        make([]*StorageRecord, 0),
	}
}
