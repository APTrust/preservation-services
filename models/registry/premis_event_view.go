package registry

import (
	"time"
)

// PremisEventView is a read-only model containing info about a
// PremisEvent.
type PremisEventView struct {
	ID                           int64     `json:"id" form:"id"`
	Agent                        string    `json:"agent"`
	CreatedAt                    time.Time `json:"created_at"`
	DateTime                     time.Time `json:"date_time"`
	Detail                       string    `json:"detail"`
	EventType                    string    `json:"event_type"`
	GenericFileID                int64     `json:"generic_file_id"`
	GenericFileIdentifier        string    `json:"generic_file_identifier"`
	Identifier                   string    `json:"identifier"`
	InstitutionID                int64     `json:"institution_id"`
	InstitutionName              string    `json:"institution_name"`
	IntellectualObjectID         int64     `json:"intellectual_object_id"`
	IntellectualObjectIdentifier string    `json:"intellectual_object_identifier"`
	Object                       string    `json:"object"`
	OldUUID                      string    `json:"old_uuid"`
	Outcome                      string    `json:"outcome"`
	OutcomeDetail                string    `json:"outcome_detail"`
	OutcomeInformation           string    `json:"outcome_information"`
	UpdatedAt                    time.Time `json:"updated_at"`
}
