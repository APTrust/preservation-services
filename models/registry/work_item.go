package registry

import (
	"encoding/json"
	"os"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/util"
)

// WorkItem is a Pharos WorkItem that describes a task to be completed
// and its current stage and status.
type WorkItem struct {
	APTrustApprover       string    `json:"aptrust_approver"`
	Action                string    `json:"action"`
	BagDate               time.Time `json:"bag_date"`
	Bucket                string    `json:"bucket"`
	CreatedAt             time.Time `json:"created_at,omitempty"`
	Date                  time.Time `json:"date"`
	ETag                  string    `json:"etag"`
	GenericFileIdentifier string    `json:"generic_file_identifier"`
	ID                    int       `json:"id,omitempty"`
	InstApprover          string    `json:"inst_approver"`
	InstitutionID         int       `json:"institution_id"`
	Name                  string    `json:"name"`
	NeedsAdminReview      bool      `json:"needs_admin_review"`
	Node                  string    `json:"node"`
	Note                  string    `json:"note"`
	ObjectIdentifier      string    `json:"object_identifier"`
	Outcome               string    `json:"outcome"`
	Pid                   int       `json:"pid"`
	QueuedAt              time.Time `json:"queued_at,omitempty"`
	Retry                 bool      `json:"retry"`
	Size                  int64     `json:"size"`
	Stage                 string    `json:"stage"`
	StageStartedAt        time.Time `json:"stage_started_at"`
	Status                string    `json:"status"`
	UpdatedAt             time.Time `json:"updated_at,omitempty"`
	User                  string    `json:"user"`
}

// WorkItemFromJSON converts a JSON representation of a WorkItem to
// a WorkItem object.
func WorkItemFromJSON(jsonData []byte) (*WorkItem, error) {
	item := &WorkItem{}
	err := json.Unmarshal(jsonData, item)
	if err != nil {
		return nil, err
	}
	return item, nil
}

// ToJSON converts a WorkItem to its JSON representation.
func (item *WorkItem) ToJSON() ([]byte, error) {
	bytes, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// ProcessingHasCompleted returns true if this WorkItem in an of
// the final states of "Succeeded", "Failed", or "Cancelled." Those
// states indicate that no further processing should occur on this
// WorkItem.
func (item *WorkItem) ProcessingHasCompleted() bool {
	return util.StringListContains(constants.CompletedStatusValues, item.Status)
}

// SerializeForPharos serializes this WorkItem to be PUT or POSTed
// to Pharos.
//
// See TODO below. This is logged in Trello as
// https://trello.com/c/ZBBxFWid
func (item *WorkItem) SerializeForPharos() ([]byte, error) {
	return json.Marshal(NewWorkItemForPharos(item))
}

// WorkItemForPharos is a struct that boils down a WorkItem object
// to the fields that Pharos will accept for PUT and POST requests.
//
// TODO: This needs to be fixed in Pharos.
// While other controllers want JSON in the format
// { "item_name": <object> }, the WorkItems controller
// takes a flat struct like the one below for POST and PUT.
// The format should be consistent across controllers.
type WorkItemForPharos struct {
	APTrustApprover       string    `json:"aptrust_approver"`
	Action                string    `json:"action"`
	BagDate               time.Time `json:"bag_date"`
	Bucket                string    `json:"bucket"`
	Date                  time.Time `json:"date"`
	ETag                  string    `json:"etag"`
	GenericFileIdentifier string    `json:"generic_file_identifier"`
	InstApprover          string    `json:"inst_approver"`
	InstitutionID         int       `json:"institution_id"`
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
}

// NewWorkItemForPharos converts WorkItem to a struct that Pharos will
// accept in PUT and POST requests.
func NewWorkItemForPharos(item *WorkItem) *WorkItemForPharos {
	return &WorkItemForPharos{
		APTrustApprover:       item.APTrustApprover,
		Action:                item.Action,
		BagDate:               item.BagDate,
		Bucket:                item.Bucket,
		Date:                  item.Date,
		ETag:                  item.ETag,
		GenericFileIdentifier: item.GenericFileIdentifier,
		InstApprover:          item.InstApprover,
		InstitutionID:         item.InstitutionID,
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
	}
}

// SetNodeAndPid sets the Node and Pid properties of this WorkItem to
// the hostname and pid of the current worker/process.
func (item *WorkItem) SetNodeAndPid() {
	hostname, _ := os.Hostname()
	item.Node = hostname
	item.Pid = os.Getpid()
}

// ClearNodeAndPid sets this WorkItem's Node to an empty string and its
// Pid to zero.
func (item *WorkItem) ClearNodeAndPid() {
	item.Node = ""
	item.Pid = 0
}

// MarkInProgress sets this WorkItem's Node and Pid, as well as the
// Stage, Status, and Note.
func (item *WorkItem) MarkInProgress(stage, status, note string) {
	item.SetNodeAndPid()
	item.Stage = stage
	item.Status = status
	item.Note = note
	item.StageStartedAt = time.Now().UTC()
}

// MarkNoLongerInProgress clears this WorkItem's Node and Pid, and sets
// the Stage, Status, and Note. The caller should also set Retry and
// NeedsAdminReview if necessary.
func (item *WorkItem) MarkNoLongerInProgress(stage, status, note string) {
	item.ClearNodeAndPid()
	item.Stage = stage
	item.Status = status
	item.Note = note
}
