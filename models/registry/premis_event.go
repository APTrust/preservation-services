package registry

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/util"
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

func NewObjectCreationEvent() *PremisEvent {
	eventId := uuid.NewV4()
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          constants.EventCreation,
		DateTime:           time.Now().UTC(),
		Detail:             "Object created",
		Outcome:            constants.StatusSuccess,
		OutcomeDetail:      "Intellectual object created",
		Object:             "APTrust preservation services",
		Agent:              "https://github.com/APTrust/preservation-services",
		OutcomeInformation: "Object created, files copied to preservation storage",
	}
}

func NewObjectIngestEvent(numberOfFilesIngested int) *PremisEvent {
	eventId := uuid.NewV4()
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          constants.EventIngestion,
		DateTime:           time.Now().UTC(),
		Detail:             "Copied files to perservation storage",
		Outcome:            constants.StatusSuccess,
		OutcomeDetail:      fmt.Sprintf("%d files copied", numberOfFilesIngested),
		Object:             "Minio S3 client",
		Agent:              "https://github.com/minio/minio-go",
		OutcomeInformation: "Multipart put using s3 etags",
	}
}

func NewObjectIdentifierEvent(objectIdentifier string) *PremisEvent {
	eventId := uuid.NewV4()
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          constants.EventIdentifierAssignment,
		DateTime:           time.Now().UTC(),
		Detail:             "Assigned object identifier " + objectIdentifier,
		Outcome:            constants.StatusSuccess,
		OutcomeDetail:      objectIdentifier,
		Object:             "APTrust preservation services",
		Agent:              "https://github.com/APTrust/preservation-services",
		OutcomeInformation: "Institution domain + tar file name",
	}
}

func NewObjectRightsEvent(accessSetting string) *PremisEvent {
	eventId := uuid.NewV4()
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          constants.EventAccessAssignment,
		DateTime:           time.Now().UTC(),
		Detail:             "Assigned object access rights",
		Outcome:            constants.StatusSuccess,
		OutcomeDetail:      accessSetting,
		Object:             "APTrust preservation services",
		Agent:              "https://github.com/APTrust/preservation-services",
		OutcomeInformation: "Set access to " + accessSetting,
	}
}

// NewFileIngestEvent returns a PremisEvent describing a file ingest.
// Param storedAt should come from the IngestFile's primary StorageRecord.
// Param md5Digest should come from the IngestFiles md5 Checksum record.
// Param _uuid should come from IngestFile.UUID.
func NewFileIngestEvent(storedAt time.Time, md5Digest, _uuid string) (*PremisEvent, error) {
	if storedAt.IsZero() {
		return nil, fmt.Errorf("Param storedAt cannot be empty.")
	}
	if len(md5Digest) != 32 {
		return nil, fmt.Errorf("Param md5Digest must have 32 characters. '%s' doesn't.",
			md5Digest)
	}
	if !util.LooksLikeUUID(_uuid) {
		return nil, fmt.Errorf("Param _uuid with value '%s' doesn't look like a uuid.",
			_uuid)
	}
	eventId := uuid.NewV4()
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          constants.EventIngestion,
		DateTime:           storedAt,
		Detail:             fmt.Sprintf("Completed copy to S3 (%s)", _uuid),
		Outcome:            constants.StatusSuccess,
		OutcomeDetail:      fmt.Sprintf("md5:%s", md5Digest),
		Object:             "preservation-services + Minio S3 client",
		Agent:              "https://github.com/minio/minio-go",
		OutcomeInformation: "Put using md5 checksum",
	}, nil
}

// NewFileDigestEvent returns a PremisEvent describing the outcome of a
// fixity check. The check may occur at ingest or on a specified schedule against
// a file in preservation storage.
//
// If this event is being generated on ingest, all params should come from
// the IngestFile's Checksum record. When this event is generated by a scheduled
// fixity check, the params will come from the outcome of the check.
func NewFileFixityCheckEvent(checksumVerifiedAt time.Time, fixityAlg, digest string, fixityMatched bool) (*PremisEvent, error) {
	if checksumVerifiedAt.IsZero() {
		return nil, fmt.Errorf("Param checksumVerifiedAt cannot be empty.")
	}
	if !util.StringListContains(constants.PreferredAlgsInOrder, fixityAlg) {
		return nil, fmt.Errorf("Param fixityAlg '%s' is not valid.", fixityAlg)
	}
	if len(digest) != 32 && len(digest) != 64 {
		return nil, fmt.Errorf("Param digest must have 32 or 64 characters. '%s' doesn't.",
			digest)
	}
	eventId := uuid.NewV4()
	props := getFixityProps(fixityAlg, true)
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          constants.EventFixityCheck,
		DateTime:           checksumVerifiedAt,
		Detail:             "Fixity check against registered hash",
		Outcome:            props["outcome"],
		OutcomeDetail:      fmt.Sprintf("%s:%s", fixityAlg, digest),
		Object:             props["object"],
		Agent:              props["agent"],
		OutcomeInformation: props["outcomeInformation"],
	}, nil
}

// NewFileDigestEvent returns a PremisEvent saying that we calculated a new
// checksum digest on this file during ingest.
//
// All params should come from the IngestFile's Checksum record.
func NewFileDigestEvent(checksumGeneratedAt time.Time, fixityAlg, digest string) (*PremisEvent, error) {
	if checksumGeneratedAt.IsZero() {
		return nil, fmt.Errorf("Param checksumVerifiedAt cannot be empty.")
	}
	if !util.StringListContains(constants.PreferredAlgsInOrder, fixityAlg) {
		return nil, fmt.Errorf("Param fixityAlg '%s' is not valid.", fixityAlg)
	}
	if len(digest) != 32 && len(digest) != 64 {
		return nil, fmt.Errorf("Param digest must have 32 or 64 characters. '%s' doesn't.",
			digest)
	}
	eventId := uuid.NewV4()
	props := getFixityProps(fixityAlg, true)
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          constants.EventDigestCalculation,
		DateTime:           checksumGeneratedAt,
		Detail:             "Calculated fixity value",
		Outcome:            props["outcome"],
		OutcomeDetail:      fmt.Sprintf("%s:%s", fixityAlg, digest),
		Object:             props["object"],
		Agent:              props["agent"],
		OutcomeInformation: "Calculated fixity value",
	}, nil
}

// NewFileIdentifierEvent returns a PremisEvent describing the identifier
// that was assigned to a file on ingest.
func NewFileIdentifierEvent(identifierGeneratedAt time.Time, identifierType, identifier string) (*PremisEvent, error) {
	if identifierGeneratedAt.IsZero() {
		return nil, fmt.Errorf("Param identifierGeneratedAt cannot be empty.")
	}
	if identifierType != constants.IdTypeStorageURL && identifierType != constants.IdTypeBagAndPath {
		return nil, fmt.Errorf("Param identifierType '%s' is not valid.", identifierType)
	}
	if identifier == "" {
		return nil, fmt.Errorf("Param identifier cannot be empty.")
	}
	eventId := uuid.NewV4()
	object := "APTrust exchange/ingest processor"
	agent := "https://github.com/APTrust/exchange"
	detail := "Assigned new institution.bag/path identifier"
	if identifierType == constants.IdTypeStorageURL {
		object = "Go uuid library + AWS Go SDK S3 library"
		agent = "http://github.com/satori/go.uuid"
		// Don't change these words. They're used in IsUrlAssignment below.
		detail = fmt.Sprintf("Assigned new storage URL identifier, and item was stored at %s",
			identifierGeneratedAt.Format(time.RFC3339))
	}
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          constants.EventIdentifierAssignment,
		DateTime:           identifierGeneratedAt,
		Detail:             detail,
		Outcome:            string(constants.StatusSuccess),
		OutcomeDetail:      identifier,
		Object:             object,
		Agent:              agent,
		OutcomeInformation: fmt.Sprintf("Assigned %s identifier", identifierType),
	}, nil
}

// NewFileReplicationEvent returns a PremisEvent describing when a file
// was copied to replication storage. Params should come from the IngestFile's
// StorageRecord that describes where and when the replication copy was stored.
func NewFileReplicationEvent(replicatedAt time.Time, replicationUrl string) (*PremisEvent, error) {
	if replicatedAt.IsZero() {
		return nil, fmt.Errorf("Param replicatedAt cannot be empty.")
	}
	if replicationUrl == "" {
		return nil, fmt.Errorf("Param identifier cannot be empty.")
	}

	eventId := uuid.NewV4()
	return &PremisEvent{
		Identifier:         eventId.String(),
		EventType:          constants.EventReplication,
		DateTime:           replicatedAt,
		Detail:             "Copied to replication storage and assigned replication URL identifier",
		Outcome:            constants.StatusSuccess,
		OutcomeDetail:      replicationUrl,
		Object:             "Go uuid library + Minio S3 library",
		Agent:              "http://github.com/satori/go.uuid",
		OutcomeInformation: "Replicated to secondary storage",
	}, nil
}

func getFixityProps(fixityAlg string, fixityMatched bool) map[string]string {
	details := make(map[string]string)
	details["object"] = "Go language crypto/md5"
	details["agent"] = "http://golang.org/pkg/crypto/md5/"
	details["outcomeInformation"] = "Fixity matches"
	details["outcome"] = string(constants.StatusSuccess)
	if fixityAlg == constants.AlgSha256 {
		details["object"] = "Go language crypto/sha256"
		details["agent"] = "http://golang.org/pkg/crypto/sha256/"
	}
	if fixityMatched == false {
		details["outcome"] = string(constants.StatusFailed)
		details["outcomeInformation"] = "Fixity did not match"
	}
	return details
}
