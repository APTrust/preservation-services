package ingest

import (
	"github.com/APTrust/preservation-services/models/bagit"
	"time"
)

type IngestObject struct {
	Access                 string               `json:"access,omitempty"`
	AltIdentifier          string               `json:"alt_identifier,omitempty"`
	BagGroupIdentifier     string               `json:"bag_group_identifier,omitempty"`
	BagItProfileIdentifier string               `json:"bagit_profile_identifier,omitempty"`
	BagName                string               `json:"bag_name,omitempty"`
	CreatedAt              time.Time            `json:"created_at,omitempty"`
	DeletedFromReceivingAt time.Time            `json:"ingest_deleted_from_receiving_at,omitempty"`
	Description            string               `json:"description,omitempty"`
	ETag                   string               `json:"etag,omitempty"`
	ErrorMessage           string               `json:"ingest_error_message,omitempty"`
	FilesIgnored           []string             `json:"ingest_files_ignored,omitempty"`
	Id                     int                  `json:"id,omitempty"`
	Identifier             string               `json:"identifier,omitempty"`
	Institution            string               `json:"institution,omitempty"`
	InstitutionId          int                  `json:"institution_id,omitempty"`
	Manifests              []string             `json:"ingest_manifests,omitempty"`
	MissingFiles           []*bagit.MissingFile `json:"ingest_missing_files,omitempty"`
	S3Bucket               string               `json:"ingest_s3_bucket,omitempty"`
	S3Key                  string               `json:"ingest_s3_key,omitempty"`
	Size                   int64                `json:"ingest_size,omitempty"`
	SourceOrganization     string               `json:"source_organization,omitempty"`
	State                  string               `json:"state"`
	StorageOption          string               `json:"storage_option"`
	TagManifests           []string             `json:"ingest_tag_manifests,omitempty"`
	Tags                   []*bagit.Tag         `json:"ingest_tags,omitempty"`
	Title                  string               `json:"title,omitempty"`
	TopLevelDirNames       []string             `json:"ingest_top_level_dir_names,omitempty"`
	UpdatedAt              time.Time            `json:"updated_at,omitempty"`
}
