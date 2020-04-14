package registry

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/APTrust/preservation-services/constants"
	uuid "github.com/satori/go.uuid"
)

type PremisEvent struct {
	Agent                        string    `json:"agent"`
	CreatedAt                    time.Time `json:"created_at,omitempty"`
	DateTime                     time.Time `json:"date_time"`
	Detail                       string    `json:"detail"`
	EventType                    string    `json:"event_type"`
	GenericFileID                int       `json:"generic_file_id,omitempty"`
	GenericFileIdentifier        string    `json:"generic_file_identifier,omitempty"`
	ID                           int       `json:"id,omitempty"`
	Identifier                   string    `json:"identifier"`
	InstitutionID                int       `json:"institution_id"`
	IntellectualObjectID         int       `json:"intellectual_object_id"`
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

func NewObjectCreationEvent(identifier string) *PremisEvent {
	eventId := uuid.NewV4()
	return &PremisEvent{
		Identifier:                   eventId.String(),
		EventType:                    constants.EventCreation,
		DateTime:                     time.Now().UTC(),
		Detail:                       "Object created",
		Outcome:                      constants.StatusSuccess,
		OutcomeDetail:                "Intellectual object created",
		Object:                       "APTrust preservation services",
		IntellectualObjectIdentifier: identifier,
		Agent:                        "https://github.com/APTrust/preservation-services",
		OutcomeInformation:           "Object created, files copied to preservation storage",
	}
}

func NewObjectIngestEvent(identifier string, numberOfFilesIngested int) *PremisEvent {
	eventId := uuid.NewV4()
	return &PremisEvent{
		Identifier:                   eventId.String(),
		EventType:                    constants.EventIngestion,
		DateTime:                     time.Now().UTC(),
		Detail:                       "Copied files to perservation storage",
		Outcome:                      constants.StatusSuccess,
		OutcomeDetail:                fmt.Sprintf("%d files copied", numberOfFilesIngested),
		Object:                       "Minio S3 client",
		IntellectualObjectIdentifier: identifier,
		Agent:                        "https://github.com/minio/minio-go",
		OutcomeInformation:           "Multipart put using s3 etags",
	}
}

func NewObjectIdentifierEvent(identifier string) *PremisEvent {
	eventId := uuid.NewV4()
	return &PremisEvent{
		Identifier:                   eventId.String(),
		EventType:                    constants.EventIdentifierAssignment,
		DateTime:                     time.Now().UTC(),
		Detail:                       "Assigned object identifier " + identifier,
		Outcome:                      constants.StatusSuccess,
		OutcomeDetail:                identifier,
		Object:                       "APTrust preservation services",
		IntellectualObjectIdentifier: identifier,
		Agent:                        "https://github.com/APTrust/preservation-services",
		OutcomeInformation:           "Institution domain + tar file name",
	}
}

func NewObjectRightsEvent(identifier, accessSetting string) *PremisEvent {
	eventId := uuid.NewV4()
	return &PremisEvent{
		Identifier:                   eventId.String(),
		EventType:                    constants.EventAccessAssignment,
		DateTime:                     time.Now().UTC(),
		Detail:                       "Assigned object access rights",
		Outcome:                      constants.StatusSuccess,
		OutcomeDetail:                accessSetting,
		Object:                       "APTrust preservation services",
		IntellectualObjectIdentifier: identifier,
		Agent:                        "https://github.com/APTrust/preservation-services",
		OutcomeInformation:           "Set access to " + accessSetting,
	}
}
