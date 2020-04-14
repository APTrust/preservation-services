package registry_test

import (
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var event = &registry.PremisEvent{
	Agent:                        "Maxwell Smart",
	CreatedAt:                    testutil.Bloomsday,
	DateTime:                     testutil.Bloomsday,
	Detail:                       "detail-123",
	EventType:                    constants.EventIngestion,
	GenericFileID:                432,
	GenericFileIdentifier:        "test.edu/bag/data/file.txt",
	Identifier:                   "uuid goes here",
	InstitutionID:                21,
	IntellectualObjectID:         3433,
	IntellectualObjectIdentifier: "test.edu/bag",
	Object:                       "object-321",
	OutcomeDetail:                "outcome detail",
	OutcomeInformation:           "outcome information",
	Outcome:                      "outcome",
	UpdatedAt:                    testutil.Bloomsday,
}

var eventJson = `{"agent":"Maxwell Smart","created_at":"1904-06-16T15:04:05Z","date_time":"1904-06-16T15:04:05Z","detail":"detail-123","event_type":"ingestion","generic_file_id":432,"generic_file_identifier":"test.edu/bag/data/file.txt","identifier":"uuid goes here","institution_id":21,"intellectual_object_id":3433,"intellectual_object_identifier":"test.edu/bag","object":"object-321","outcome_detail":"outcome detail","outcome_information":"outcome information","outcome":"outcome","updated_at":"1904-06-16T15:04:05Z"}`

const eObjIdent = "test.edu/obj"
const eFileIdent = "test.edu/obj/file.txt"

func TestPremisEventFromJson(t *testing.T) {
	premisEvent, err := registry.PremisEventFromJSON([]byte(eventJson))
	require.Nil(t, err)
	assert.Equal(t, event, premisEvent)
}

func TestPremisEventToJson(t *testing.T) {
	actualJson, err := event.ToJSON()
	require.Nil(t, err)
	assert.Equal(t, eventJson, string(actualJson))
}

// Pharos uses standard JSON format for this model.
func TestPremisEventSerializeForPharos(t *testing.T) {
	actualJson, err := event.SerializeForPharos()
	require.Nil(t, err)
	assert.Equal(t, eventJson, string(actualJson))
}

func TestNewObjectCreationEvent(t *testing.T) {
	event := registry.NewObjectCreationEvent("test.edu/obj")
	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventCreation, event.EventType)
	assert.False(t, event.DateTime.IsZero())
	assert.Equal(t, "Object created", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "Intellectual object created", event.OutcomeDetail)
	assert.Equal(t, "APTrust preservation services", event.Object)
	assert.Equal(t, "test.edu/obj", event.IntellectualObjectIdentifier)
	assert.Equal(t, "https://github.com/APTrust/preservation-services", event.Agent)
	assert.Equal(t, "Object created, files copied to preservation storage", event.OutcomeInformation)
}

func TestNewObjectIngestEvent(t *testing.T) {
	event := registry.NewObjectIngestEvent("test.edu/obj", 12)
	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventIngestion, event.EventType)
	assert.False(t, event.DateTime.IsZero())
	assert.Equal(t, "Copied files to perservation storage", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "12 files copied", event.OutcomeDetail)
	assert.Equal(t, "Minio S3 client", event.Object)
	assert.Equal(t, "test.edu/obj", event.IntellectualObjectIdentifier)
	assert.Equal(t, "https://github.com/minio/minio-go", event.Agent)
	assert.Equal(t, "Multipart put using s3 etags", event.OutcomeInformation)
}

func TestNewObjectIdentifierEvent(t *testing.T) {
	event := registry.NewObjectIdentifierEvent("test.edu/obj")
	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventIdentifierAssignment, event.EventType)
	assert.False(t, event.DateTime.IsZero())
	assert.Equal(t, "Assigned object identifier test.edu/obj", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "test.edu/obj", event.OutcomeDetail)
	assert.Equal(t, "APTrust preservation services", event.Object)
	assert.Equal(t, "test.edu/obj", event.IntellectualObjectIdentifier)
	assert.Equal(t, "https://github.com/APTrust/preservation-services", event.Agent)
	assert.Equal(t, "Institution domain + tar file name", event.OutcomeInformation)
}

func TestNewObjectRightsEvent(t *testing.T) {
	event := registry.NewObjectRightsEvent("test.edu/obj", "restricted")
	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventAccessAssignment, event.EventType)
	assert.False(t, event.DateTime.IsZero())
	assert.Equal(t, "Assigned object access rights", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "restricted", event.OutcomeDetail)
	assert.Equal(t, "APTrust preservation services", event.Object)
	assert.Equal(t, "test.edu/obj", event.IntellectualObjectIdentifier)
	assert.Equal(t, "https://github.com/APTrust/preservation-services", event.Agent)
	assert.Equal(t, "Set access to restricted", event.OutcomeInformation)
}
