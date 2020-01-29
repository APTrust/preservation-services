package registry

import (
	"encoding/json"
	"time"
)

type WorkItem struct {
	APTrustApprover       string    `json:"aptrust_approver"`
	Action                string    `json:"action"`
	BagDate               time.Time `json:"bag_date"`
	Bucket                string    `json:"bucket"`
	CreatedAt             time.Time `json:"created_at"`
	Date                  time.Time `json:"date"`
	ETag                  string    `json:"etag"`
	GenericFileId         int       `json:"generic_file_id"`
	GenericFileIdentifier string    `json:"generic_file_identifier"`
	Id                    int       `json:"id"`
	InstApprover          string    `json:"inst_appropver"`
	InstitutionId         int       `json:"institution_id"`
	Name                  string    `json:"name"`
	NeedsAdminReview      bool      `json:"needs_admin_review"`
	Node                  string    `json:"node"`
	Note                  string    `json:"note"`
	ObjectIdentifier      string    `json:"object_identifier"`
	Outcome               string    `json:"outcome"`
	Pid                   int       `json:"pid"`
	QueuedAt              time.Time `json:"queued_at"`
	Retry                 bool      `json:"retry"`
	Size                  int64     `json:"size"`
	Stage                 string    `json:"stage"`
	StageStartedAt        time.Time `json:"stage_started_at"`
	Status                string    `json:"status"`
	UpdatedAt             time.Time `json:"updated_at"`
	User                  string    `json:"user"`
}

func WorkItemFromJson(jsonData []byte) (*WorkItem, error) {
	item := &WorkItem{}
	err := json.Unmarshal(jsonData, item)
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (item *WorkItem) ToJson() ([]byte, error) {
	bytes, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}
