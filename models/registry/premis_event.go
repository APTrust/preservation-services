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
	GenericFileId                int       `json:"generic_file_id,omitempty"`
	GenericFileIdentifier        string    `json:"generic_file_identifier,omitempty"`
	Id                           int       `json:"id,omitempty"`
	Identifier                   string    `json:"identifier"`
	InstitutionId                int       `json:"institution_id"`
	IntellectualObjectId         int       `json:"intellectual_object_id"`
	IntellectualObjectIdentifier string    `json:"intellectual_object_identifier"`
	Object                       string    `json:"object"`
	OutcomeDetail                string    `json:"outcome_detail"`
	OutcomeInformation           string    `json:"outcome_information"`
	Outcome                      string    `json:"outcome"`
	UpdatedAt                    time.Time `json:"datetime,omitempty"`
}

func PremisEventFromJson(jsonData []byte) (*PremisEvent, error) {
	event := &PremisEvent{}
	err := json.Unmarshal(jsonData, event)
	if err != nil {
		return nil, err
	}
	return event, nil
}

func (event *PremisEvent) ToJson() ([]byte, error) {
	bytes, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// JSON format for Pharos post/put is {"premis_event": <object>}
func (event *PremisEvent) SerializeForPharos() ([]byte, error) {
	dataStruct := make(map[string]*PremisEvent)
	dataStruct["premis_event"] = event
	return json.Marshal(dataStruct)
}
