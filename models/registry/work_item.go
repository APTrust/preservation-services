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
	CreatedAt             time.Time `json:"created_at,omitempty"`
	Date                  time.Time `json:"date"`
	ETag                  string    `json:"etag"`
	GenericFileIdentifier string    `json:"generic_file_identifier"`
	Id                    int       `json:"id,omitempty"`
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
	UpdatedAt             time.Time `json:"updated_at,omitempty"`
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

func (item *WorkItem) SerializeForPharos() ([]byte, error) {
	return json.Marshal(&struct {
		APTrustApprover       string    `json:"aptrust_approver"`
		Action                string    `json:"action"`
		BagDate               time.Time `json:"bag_date"`
		Bucket                string    `json:"bucket"`
		Date                  time.Time `json:"date"`
		ETag                  string    `json:"etag"`
		GenericFileIdentifier string    `json:"generic_file_identifier"`
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
		User                  string    `json:"user"`
	}{
		APTrustApprover:       item.APTrustApprover,
		Action:                item.Action,
		BagDate:               item.BagDate,
		Bucket:                item.Bucket,
		Date:                  item.Date,
		ETag:                  item.ETag,
		GenericFileIdentifier: item.GenericFileIdentifier,
		InstApprover:          item.InstApprover,
		InstitutionId:         item.InstitutionId,
		Name:                  item.Name,
		NeedsAdminReview:      item.NeedsAdminReview,
		Node:                  item.Node,
		Note:                  item.Note,
		ObjectIdentifier:      item.ObjectIdentifier,
		Outcome:               item.Outcome,
		Pid:                   item.Pid,
		QueuedAt:              item.QueuedAt,
		Retry:                 item.Retry,
		Size:                  item.Size,
		Stage:                 item.Stage,
		StageStartedAt:        item.StageStartedAt,
		Status:                item.Status,
		User:                  item.User,
	})
}
