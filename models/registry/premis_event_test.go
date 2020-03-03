package registry_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

var event = &registry.PremisEvent{
	Agent:                        "Maxwell Smart",
	CreatedAt:                    testutil.Bloomsday,
	DateTime:                     testutil.Bloomsday,
	Detail:                       "detail-123",
	EventType:                    constants.EventIngestion,
	GenericFileId:                432,
	GenericFileIdentifier:        "test.edu/bag/data/file.txt",
	Identifier:                   "uuid goes here",
	InstitutionId:                21,
	IntellectualObjectId:         3433,
	IntellectualObjectIdentifier: "test.edu/bag",
	Object:                       "object-321",
	OutcomeDetail:                "outcome detail",
	OutcomeInformation:           "outcome information",
	Outcome:                      "outcome",
	UpdatedAt:                    testutil.Bloomsday,
}

var eventJson = `{"agent":"Maxwell Smart","created_at":"1904-06-16T15:04:05Z","date_time":"1904-06-16T15:04:05Z","detail":"detail-123","event_type":"ingestion","generic_file_id":432,"generic_file_identifier":"test.edu/bag/data/file.txt","identifier":"uuid goes here","institution_id":21,"intellectual_object_id":3433,"intellectual_object_identifier":"test.edu/bag","object":"object-321","outcome_detail":"outcome detail","outcome_information":"outcome information","outcome":"outcome","updated_at":"1904-06-16T15:04:05Z"}`

// JSON format for Pharos post/put is {"premis_event": <object>}
var eventJsonForPharos = `{"premis_event":{"agent":"Maxwell Smart","created_at":"1904-06-16T15:04:05Z","date_time":"1904-06-16T15:04:05Z","detail":"detail-123","event_type":"ingestion","generic_file_id":432,"generic_file_identifier":"test.edu/bag/data/file.txt","identifier":"uuid goes here","institution_id":21,"intellectual_object_id":3433,"intellectual_object_identifier":"test.edu/bag","object":"object-321","outcome_detail":"outcome detail","outcome_information":"outcome information","outcome":"outcome","updated_at":"1904-06-16T15:04:05Z"}}`

func TestPremisEventFromJson(t *testing.T) {
	premisEvent, err := registry.PremisEventFromJson([]byte(eventJson))
	require.Nil(t, err)
	assert.Equal(t, event, premisEvent)
}

func TestPremisEventToJson(t *testing.T) {
	actualJson, err := event.ToJson()
	require.Nil(t, err)
	assert.Equal(t, eventJson, string(actualJson))
}

func TestPremisEventSerializeForPharos(t *testing.T) {
	actualJson, err := event.SerializeForPharos()
	require.Nil(t, err)
	assert.Equal(t, eventJsonForPharos, string(actualJson))
}
