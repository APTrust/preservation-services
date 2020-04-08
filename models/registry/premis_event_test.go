package registry_test

import (
	"strings"
	"testing"
	"time"

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
	event := registry.NewObjectCreationEvent()
	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventCreation, event.EventType)
	assert.False(t, event.DateTime.IsZero())
	assert.Equal(t, "Object created", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "Intellectual object created", event.OutcomeDetail)
	assert.Equal(t, "APTrust preservation services", event.Object)
	assert.Equal(t, "https://github.com/APTrust/preservation-services", event.Agent)
	assert.Equal(t, "Object created, files copied to preservation storage", event.OutcomeInformation)
}

func TestNewObjectIngestEvent(t *testing.T) {
	event := registry.NewObjectIngestEvent(12)
	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventIngestion, event.EventType)
	assert.False(t, event.DateTime.IsZero())
	assert.Equal(t, "Copied files to perservation storage", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "12 files copied", event.OutcomeDetail)
	assert.Equal(t, "Minio S3 client", event.Object)
	assert.Equal(t, "https://github.com/minio/minio-go", event.Agent)
	assert.Equal(t, "Multipart put using s3 etags", event.OutcomeInformation)
}

func TestNewObjectIdentifierEvent(t *testing.T) {
	event := registry.NewObjectIdentifierEvent("test.edu/some-object")
	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventIdentifierAssignment, event.EventType)
	assert.False(t, event.DateTime.IsZero())
	assert.Equal(t, "Assigned object identifier test.edu/some-object", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "test.edu/some-object", event.OutcomeDetail)
	assert.Equal(t, "APTrust preservation services", event.Object)
	assert.Equal(t, "https://github.com/APTrust/preservation-services", event.Agent)
	assert.Equal(t, "Institution domain + tar file name", event.OutcomeInformation)
}

func TestNewObjectRightsEvent(t *testing.T) {
	event := registry.NewObjectRightsEvent("restricted")
	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventAccessAssignment, event.EventType)
	assert.False(t, event.DateTime.IsZero())
	assert.Equal(t, "Assigned object access rights", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "restricted", event.OutcomeDetail)
	assert.Equal(t, "APTrust preservation services", event.Object)
	assert.Equal(t, "https://github.com/APTrust/preservation-services", event.Agent)
	assert.Equal(t, "Set access to restricted", event.OutcomeInformation)
}

func TestNewFileIngestEvent(t *testing.T) {
	event, err := registry.NewFileIngestEvent(testutil.Bloomsday, testutil.EmptyMd5, constants.EmptyUUID)
	require.Nil(t, err)
	require.NotNil(t, event)

	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventIngestion, event.EventType)
	assert.Equal(t, testutil.Bloomsday, event.DateTime)
	assert.Equal(t, "Completed copy to preservation storage (00000000-0000-0000-0000-000000000000)", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "md5:00000000000000000000000000000000", event.OutcomeDetail)
	assert.Equal(t, "preservation-services + Minio S3 client", event.Object)
	assert.Equal(t, "https://github.com/minio/minio-go", event.Agent)
	assert.Equal(t, "Put using md5 checksum", event.OutcomeInformation)

	event, err = registry.NewFileIngestEvent(time.Time{}, testutil.EmptyMd5, constants.EmptyUUID)
	require.Nil(t, event)
	require.NotNil(t, err)
	assert.Equal(t, "Param storedAt cannot be empty.", err.Error())

	event, err = registry.NewFileIngestEvent(testutil.Bloomsday, "", constants.EmptyUUID)
	require.Nil(t, event)
	require.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "Param md5Digest must have 32 characters"))

	event, err = registry.NewFileIngestEvent(testutil.Bloomsday, testutil.EmptyMd5, "xyz")
	require.Nil(t, event)
	require.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "doesn't look like a uuid"))
}

func TestNewFileFixityCheckEvent(t *testing.T) {
	event, err := registry.NewFileFixityCheckEvent(testutil.Bloomsday, constants.AlgMd5, testutil.EmptyMd5, true)
	require.Nil(t, err)
	require.NotNil(t, event)

	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventFixityCheck, event.EventType)
	assert.Equal(t, testutil.Bloomsday, event.DateTime)
	assert.Equal(t, "Fixity check against registered hash", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "md5:00000000000000000000000000000000", event.OutcomeDetail)
	assert.Equal(t, "Go language crypto/md5", event.Object)
	assert.Equal(t, "http://golang.org/pkg/crypto/md5/", event.Agent)
	assert.Equal(t, "Fixity matches", event.OutcomeInformation)

	event, err = registry.NewFileFixityCheckEvent(testutil.Bloomsday, constants.AlgSha256, testutil.EmptySha256, false)
	require.Nil(t, err)
	require.NotNil(t, event)

	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventFixityCheck, event.EventType)
	assert.Equal(t, testutil.Bloomsday, event.DateTime)
	assert.Equal(t, "Fixity check against registered hash", event.Detail)
	assert.Equal(t, constants.StatusFailed, event.Outcome)
	assert.Equal(t, "sha256:0000000000000000000000000000000000000000000000000000000000000000", event.OutcomeDetail)
	assert.Equal(t, "Go language crypto/sha256", event.Object)
	assert.Equal(t, "http://golang.org/pkg/crypto/sha256/", event.Agent)
	assert.Equal(t, "Fixity did not match", event.OutcomeInformation)

	event, err = registry.NewFileFixityCheckEvent(time.Time{}, constants.AlgMd5, testutil.EmptyMd5, true)
	require.Nil(t, event)
	require.NotNil(t, err)
	assert.Equal(t, "Param checksumVerifiedAt cannot be empty.", err.Error())

	event, err = registry.NewFileFixityCheckEvent(testutil.Bloomsday, "", testutil.EmptyMd5, true)
	require.Nil(t, event)
	require.NotNil(t, err)
	assert.Equal(t, "Param fixityAlg '' is not valid.", err.Error())

	event, err = registry.NewFileFixityCheckEvent(testutil.Bloomsday, constants.AlgMd5, "xyz", true)
	require.Nil(t, event)
	require.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "Param digest must have 32, 64, or 128 characters"))

}

func TestNewFileDigestEvent(t *testing.T) {
	event, err := registry.NewFileDigestEvent(testutil.Bloomsday, constants.AlgSha256, testutil.EmptySha256)
	require.Nil(t, err)
	require.NotNil(t, event)

	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventDigestCalculation, event.EventType)
	assert.Equal(t, testutil.Bloomsday, event.DateTime)
	assert.Equal(t, "Calculated fixity value", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "sha256:0000000000000000000000000000000000000000000000000000000000000000", event.OutcomeDetail)
	assert.Equal(t, "Go language crypto/sha256", event.Object)
	assert.Equal(t, "http://golang.org/pkg/crypto/sha256/", event.Agent)
	assert.Equal(t, "Calculated fixity value", event.OutcomeInformation)

	event, err = registry.NewFileDigestEvent(testutil.Bloomsday, constants.AlgMd5, testutil.EmptyMd5)
	require.Nil(t, err)
	require.NotNil(t, event)

	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventDigestCalculation, event.EventType)
	assert.Equal(t, testutil.Bloomsday, event.DateTime)
	assert.Equal(t, "Calculated fixity value", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "md5:00000000000000000000000000000000", event.OutcomeDetail)
	assert.Equal(t, "Go language crypto/md5", event.Object)
	assert.Equal(t, "http://golang.org/pkg/crypto/md5/", event.Agent)
	assert.Equal(t, "Calculated fixity value", event.OutcomeInformation)

	event, err = registry.NewFileDigestEvent(time.Time{}, constants.AlgMd5, testutil.EmptyMd5)
	require.Nil(t, event)
	require.NotNil(t, err)
	assert.Equal(t, "Param checksumGeneratedAt cannot be empty.", err.Error())

	event, err = registry.NewFileDigestEvent(testutil.Bloomsday, "", testutil.EmptyMd5)
	require.Nil(t, event)
	require.NotNil(t, err)
	assert.Equal(t, "Param fixityAlg '' is not valid.", err.Error())

	event, err = registry.NewFileDigestEvent(testutil.Bloomsday, constants.AlgMd5, "xyz")
	require.Nil(t, event)
	require.NotNil(t, err)
	assert.Equal(t, "Param digest must have 32, 64 or 128 characters. 'xyz' doesn't.", err.Error())

}

func TestNewFileIdentifierEvent(t *testing.T) {
	event, err := registry.NewFileIdentifierEvent(testutil.Bloomsday, constants.IdTypeBagAndPath, "test.edu/bag/data/file.txt")
	require.Nil(t, err)
	require.NotNil(t, event)

	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventIdentifierAssignment, event.EventType)
	assert.Equal(t, testutil.Bloomsday, event.DateTime)
	assert.Equal(t, "Assigned new institution.bag/path identifier", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "test.edu/bag/data/file.txt", event.OutcomeDetail)
	assert.Equal(t, "APTrust exchange/ingest processor", event.Object)
	assert.Equal(t, "https://github.com/APTrust/preservation-services", event.Agent)
	assert.Equal(t, "Assigned bag/filepath identifier", event.OutcomeInformation)

	event, err = registry.NewFileIdentifierEvent(testutil.Bloomsday, constants.IdTypeStorageURL, "https://example.com/7890")
	require.Nil(t, err)
	require.NotNil(t, event)

	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventIdentifierAssignment, event.EventType)
	assert.Equal(t, testutil.Bloomsday, event.DateTime)
	assert.Equal(t, "Assigned new storage URL identifier, and item was stored at 1904-06-16T15:04:05Z", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "https://example.com/7890", event.OutcomeDetail)
	assert.Equal(t, "Go uuid library + Minio S3 library", event.Object)
	assert.Equal(t, "http://github.com/satori/go.uuid", event.Agent)
	assert.Equal(t, "Assigned url identifier", event.OutcomeInformation)

	event, err = registry.NewFileIdentifierEvent(time.Time{}, constants.IdTypeStorageURL, "https://example.com/7890")
	require.Nil(t, event)
	require.NotNil(t, err)
	assert.Equal(t, "Param identifierGeneratedAt cannot be empty.", err.Error())

	event, err = registry.NewFileIdentifierEvent(testutil.Bloomsday, "", testutil.EmptyMd5)
	require.Nil(t, event)
	require.NotNil(t, err)
	assert.Equal(t, "Param identifierType '' is not valid.", err.Error())

	event, err = registry.NewFileIdentifierEvent(testutil.Bloomsday, constants.IdTypeStorageURL, "")
	require.Nil(t, event)
	require.NotNil(t, err)
	assert.Equal(t, "Param identifier cannot be empty.", err.Error())
}

func TestNewFileReplicationEvent(t *testing.T) {
	event, err := registry.NewFileReplicationEvent(testutil.Bloomsday, "https://example.com/preservation/54321")
	require.Nil(t, err)
	require.NotNil(t, event)

	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventReplication, event.EventType)
	assert.Equal(t, testutil.Bloomsday, event.DateTime)
	assert.Equal(t, "Copied to replication storage and assigned replication URL identifier", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "https://example.com/preservation/54321", event.OutcomeDetail)
	assert.Equal(t, "Go uuid library + Minio S3 library", event.Object)
	assert.Equal(t, "http://github.com/satori/go.uuid", event.Agent)
	assert.Equal(t, "Replicated to secondary storage", event.OutcomeInformation)

	event, err = registry.NewFileReplicationEvent(time.Time{}, "https://example.com/7890")
	require.Nil(t, event)
	require.NotNil(t, err)
	assert.Equal(t, "Param replicatedAt cannot be empty.", err.Error())

	event, err = registry.NewFileReplicationEvent(testutil.Bloomsday, "")
	require.Nil(t, event)
	require.NotNil(t, err)
	assert.Equal(t, "Param identifier cannot be empty.", err.Error())

}
