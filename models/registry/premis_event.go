package registry

import (
	"encoding/json"
	"time"
)

type PremisEvent struct {
	Agent                        string    `json:"agent"`
	CreatedAt                    time.Time `json:"created_at,omitempty"`
	DateTime                     time.Time `json:"date_time"`
	Detail                       string    `json:"detail"`
	EventType                    string    `json:"event_type"`
	GenericFileID                int64     `json:"generic_file_id,omitempty"`
	GenericFileIdentifier        string    `json:"generic_file_identifier,omitempty"`
	ID                           int64     `json:"id,omitempty"`
	Identifier                   string    `json:"identifier"`
	InstitutionID                int64     `json:"institution_id"`
	IntellectualObjectID         int64     `json:"intellectual_object_id"`
	IntellectualObjectIdentifier string    `json:"intellectual_object_identifier"`
	Object                       string    `json:"object"`
	OutcomeDetail                string    `json:"outcome_detail"`
	OutcomeInformation           string    `json:"outcome_information"`
	Outcome                      string    `json:"outcome"`
	UpdatedAt                    time.Time `json:"updated_at,omitempty"`
}

func PremisEventFromJSON(jsonData []byte) (*PremisEvent, error) {
	event := &PremisEvent{}
	err := json.Unmarshal(jsonData, event)
	if err != nil {
		return nil, err
	}
	return event, nil
}

func (event *PremisEvent) ToJSON() ([]byte, error) {
	bytes, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// Note that Pharos uses the same format as ToJson() for this
// object.
func (event *PremisEvent) SerializeForPharos() ([]byte, error) {
	return event.ToJSON()
}
