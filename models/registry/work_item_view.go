package registry

import (
	"time"
)

// WorkItemView is a read-only model for querying. It flattens out
// WorkItem and some of its one-to-one relations for easy querying.
type WorkItemView struct {
	ID                       int64     `json:"id"`
	Name                     string    `json:"name"`
	ETag                     string    `json:"etag"`
	InstitutionID            int64     `json:"institution_id"`
	InstitutionName          string    `json:"institution_name"`
	InstitutionIdentifier    string    `json:"institution_identifier"`
	IntellectualObjectID     int64     `json:"intellectual_object_id"`
	ObjectIdentifier         string    `json:"object_identifier"`
	AltIdentifier            string    `json:"alt_identifier"`
	BagGroupIdentifier       string    `json:"bag_group_identifier"`
	StorageOption            string    `json:"storage_option"`
	BagItProfileIdentifier   string    `json:"bagit_profile_identifier"`
	SourceOrganization       string    `json:"source_organization"`
	InternalSenderIdentifier string    `json:"internal_sender_identifier"`
	GenericFileID            int64     `json:"generic_file_id"`
	GenericFileIdentifier    string    `json:"generic_file_identifier"`
	Bucket                   string    `json:"bucket"`
	User                     string    `json:"user"`
	Note                     string    `json:"note"`
	Action                   string    `json:"action"`
	Stage                    string    `json:"stage"`
	Status                   string    `json:"status"`
	Outcome                  string    `json:"outcome"`
	BagDate                  time.Time `json:"bag_date"`
	DateProcessed            time.Time `json:"date_processed"`
	Retry                    bool      `json:"retry"`
	Node                     string    `json:"node"`
	PID                      int       `json:"pid"`
	NeedsAdminReview         bool      `json:"needs_admin_review"`
	QueuedAt                 time.Time `json:"queued_at"`
	Size                     int64     `json:"size"`
	StageStartedAt           time.Time `json:"stage_started_at"`
	APTrustApprover          string    `json:"aptrust_approver"`
	InstApprover             string    `json:"inst_approver"`
	CreatedAt                time.Time `json:"created_at"`
	UpdatedAt                time.Time `json:"updated_at"`
}
