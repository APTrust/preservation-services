package service_test

import (
	"testing"

	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var etag = "12345678"
var objIdentifier = "test.edu/test-bag"
var institution = "test.edu"
var institutionId = 9855
var bucket = "bucket"
var s3Key = "test-bag.b001.of200.tar"

var internalSenderIdentifier = "Wile E. Coyote"
var bagGroupIdentifier = "Road Runner"
var sourceOrganization = "Acme Corp."
var title = "Title of Object"
var description = "Description of Object"
var internalDescription = "Description of Object (Internal Sender Description)"
var externalDescription = "Description of Object (External Description)"
var externalIdentifier = "ext-identifier"

func getObjectWithTags() *service.IngestObject {
	tags := make([]*bagit.Tag, 13)
	tags[0] = bagit.NewTag("bag-info.txt", "label1", "value1")
	tags[1] = bagit.NewTag("bag-info.txt", "label1", "value2")
	tags[2] = bagit.NewTag("bag-info.txt", "label3", "value3")
	tags[3] = bagit.NewTag("bag-info.txt", "BagIt-Profile-Identifier", constants.DefaultProfileIdentifier)
	tags[4] = bagit.NewTag("bag-info.txt", "Internal-Sender-Identifier", internalSenderIdentifier)
	tags[5] = bagit.NewTag("bag-info.txt", "Internal-Sender-Description", internalDescription)
	tags[6] = bagit.NewTag("bag-info.txt", "External-Description", externalDescription)
	tags[7] = bagit.NewTag("bag-info.txt", "External-Identifier", externalIdentifier)
	tags[8] = bagit.NewTag("bag-info.txt", "Bag-Group-Identifier", bagGroupIdentifier)
	tags[9] = bagit.NewTag("bag-info.txt", "Source-Organization", sourceOrganization)
	tags[10] = bagit.NewTag("aptrust-info.txt", "Access", constants.AccessConsortia)
	tags[11] = bagit.NewTag("aptrust-info.txt", "Title", title)
	tags[12] = bagit.NewTag("aptrust-info.txt", "Description", description)

	obj := testutil.GetIngestObject()
	obj.FileCount = 12
	obj.Tags = append(obj.Tags, tags...)
	return obj
}

func TestNewIngestObject(t *testing.T) {
	obj := service.NewIngestObject(bucket, s3Key, etag, institution, institutionId, int64(500))
	assert.Equal(t, etag, obj.ETag)
	assert.Equal(t, objIdentifier, obj.Identifier())
	assert.Equal(t, institution, obj.Institution)
	assert.NotNil(t, obj.Manifests)
	assert.NotNil(t, obj.ParsableTagFiles)
	assert.Equal(t, bucket, obj.S3Bucket)
	assert.Equal(t, s3Key, obj.S3Key)
	assert.EqualValues(t, 500, obj.Size)
	assert.NotNil(t, obj.TagManifests)
	assert.NotNil(t, obj.Tags)
	assert.False(t, obj.ShouldDeleteFromReceiving)
}

func TestIngestObjectBagName(t *testing.T) {
	obj := service.NewIngestObject(bucket, s3Key, etag, institution, institutionId, int64(500))
	assert.Equal(t, "test-bag", obj.BagName())

	obj.S3Key = "photos.tar"
	assert.Equal(t, "photos", obj.BagName())
}

func TestBaseNameOfS3Key(t *testing.T) {
	obj := service.NewIngestObject(bucket, s3Key, etag, institution, institutionId, int64(500))
	assert.Equal(t, "test-bag.b001.of200", obj.BaseNameOfS3Key())

	obj.S3Key = "photos.tar"
	assert.Equal(t, "photos", obj.BaseNameOfS3Key())
}

func TestIngestObjectIdentifier(t *testing.T) {
	obj := service.NewIngestObject(bucket, s3Key, etag, institution, institutionId, int64(500))
	assert.Equal(t, objIdentifier, obj.Identifier())

	obj.Institution = "example.edu"
	obj.S3Key = "photos.tar"
	assert.Equal(t, "example.edu/photos", obj.Identifier())
}

func TestObjFromJson(t *testing.T) {
	expectedObj := testutil.GetIngestObject()
	obj, err := service.IngestObjectFromJSON(IngestObjectJson)
	assert.Nil(t, err)
	assert.Equal(t, expectedObj.Identifier(), obj.Identifier())
	assert.Equal(t, expectedObj.ParsableTagFiles, obj.ParsableTagFiles)
}

func TestObjToJson(t *testing.T) {
	obj := testutil.GetIngestObject()
	data, err := obj.ToJSON()
	assert.Nil(t, err)
	assert.Equal(t, IngestObjectJson, data)
}

func TestGetTags(t *testing.T) {
	obj := getObjectWithTags()
	label1Tags := obj.GetTags("bag-info.txt", "label1")
	require.Equal(t, 2, len(label1Tags))
	for _, tag := range label1Tags {
		assert.Equal(t, "bag-info.txt", tag.TagFile)
		assert.Equal(t, "label1", tag.TagName)
	}
	assert.Equal(t, "value1", label1Tags[0].Value)
	assert.Equal(t, "value2", label1Tags[1].Value)

	assert.Equal(t, 1, len(obj.GetTags("aptrust-info.txt", "Access")))
	assert.Equal(t, 0, len(obj.GetTags("bag-info.txt", "Does-Not-Exist")))
}

func TestGetTag(t *testing.T) {
	obj := getObjectWithTags()
	tag := obj.GetTag("bag-info.txt", "label1")
	assert.Equal(t, "bag-info.txt", tag.TagFile)
	assert.Equal(t, "label1", tag.TagName)
	assert.Equal(t, "value1", tag.Value)

	tag = obj.GetTag("aptrust-info.txt", "Access")
	assert.Equal(t, "aptrust-info.txt", tag.TagFile)
	assert.Equal(t, "Access", tag.TagName)
	assert.Equal(t, constants.AccessConsortia, tag.Value)

	assert.Nil(t, obj.GetTag("bag-info.txt", "Does-Not-Exist"))
}

func TestBagItProfileFormat(t *testing.T) {
	obj := testutil.GetIngestObject()

	// If no BagIt-Profile-Identifier tag, should return default
	assert.Equal(t, constants.BagItProfileDefault, obj.BagItProfileFormat())

	// Set explicitly to APTrust profile
	tag := bagit.NewTag(
		"bag-info.txt",
		"BagIt-Profile-Identifier",
		"https://wiki.aptrust.org/APTrust_BagIt_Profile-2.2")
	obj.Tags = append(obj.Tags, tag)
	assert.Equal(t, constants.BagItProfileDefault, obj.BagItProfileFormat())

	// Set explicitly to BTR profile
	tag.Value = "https://raw.githubusercontent.com/dpscollaborative/btr_bagit_profile/master/btr-bagit-profile.json"
	assert.Equal(t, constants.BagItProfileBTR, obj.BagItProfileFormat())
}

func TestGetTagValue(t *testing.T) {
	obj := getObjectWithTags()
	assert.Equal(t, "value1", obj.GetTagValue("bag-info.txt", "label1", "default"))
	assert.Equal(t, "value3", obj.GetTagValue("bag-info.txt", "label3", "default"))
	assert.Equal(t, "default", obj.GetTagValue("bag-info.txt", "does-not-exist", "default"))
}

func TestAccess(t *testing.T) {
	obj := getObjectWithTags()
	assert.Equal(t, constants.AccessConsortia, obj.Access())
	obj.Tags = make([]*bagit.Tag, 0)
	assert.Equal(t, constants.DefaultAccess, obj.Access())
}

func TestAltIdentifier(t *testing.T) {
	obj := getObjectWithTags()
	assert.Equal(t, internalSenderIdentifier, obj.AltIdentifier())
}

func TestBagGroupIdentifier(t *testing.T) {
	obj := getObjectWithTags()
	assert.Equal(t, bagGroupIdentifier, obj.BagGroupIdentifier())
}

func TestBagItProfileIdentifier(t *testing.T) {
	obj := getObjectWithTags()
	assert.Equal(t, constants.DefaultProfileIdentifier, obj.BagItProfileIdentifier())

	tag := obj.GetTag("bag-info.txt", "BagIt-Profile-Identifier")
	tag.Value = "https://example.com/profile.json"
	assert.Equal(t, "https://example.com/profile.json", obj.BagItProfileIdentifier())

	obj.Tags = make([]*bagit.Tag, 0)
	assert.Equal(t, constants.DefaultProfileIdentifier, obj.BagItProfileIdentifier())
}

func TestDescription(t *testing.T) {
	obj := getObjectWithTags()
	assert.Equal(t, description, obj.Description())
}

func TestExternalDescription(t *testing.T) {
	obj := getObjectWithTags()
	assert.Equal(t, externalDescription, obj.ExternalDescription())
}

func TestExternalIdentifier(t *testing.T) {
	obj := getObjectWithTags()
	assert.Equal(t, externalIdentifier, obj.ExternalIdentifier())
}

func TestInternalSenderDescription(t *testing.T) {
	obj := getObjectWithTags()
	assert.Equal(t, internalDescription, obj.InternalSenderDescription())
}

func TestSourceOrganization(t *testing.T) {
	obj := getObjectWithTags()
	assert.Equal(t, sourceOrganization, obj.SourceOrganization())
}

func TestBestAvailableDescription(t *testing.T) {
	obj := getObjectWithTags()
	aptDescTag := obj.GetTag("aptrust-info.txt", "Description")
	intDescTag := obj.GetTag("bag-info.txt", "Internal-Sender-Description")
	extDescTag := obj.GetTag("bag-info.txt", "External-Description")

	// If all descriptions are available, it should choose APTrust desc.
	assert.Equal(t, description, obj.BestAvailableDescription())

	// If no APTrust desc, choose internal sender desc
	aptDescTag.Value = ""
	assert.Equal(t, internalDescription, obj.BestAvailableDescription())

	// If no internal desc, choose external desc
	intDescTag.Value = ""
	assert.Equal(t, externalDescription, obj.BestAvailableDescription())

	// Else empty
	extDescTag.Value = ""
	assert.Equal(t, "", obj.BestAvailableDescription())
}

func TestToIntellectualObject(t *testing.T) {
	obj := getObjectWithTags()
	intelObj := obj.ToIntellectualObject()
	assert.Equal(t, "consortia", intelObj.Access)
	assert.Equal(t, internalSenderIdentifier, intelObj.AltIdentifier)
	assert.Equal(t, bagGroupIdentifier, intelObj.BagGroupIdentifier)
	assert.Equal(t, "some-bag", intelObj.BagName)
	assert.Equal(t, description, intelObj.Description)
	assert.Equal(t, etag, intelObj.ETag)
	assert.Equal(t, obj.ID, intelObj.ID)
	assert.Equal(t, "test.edu/some-bag", intelObj.Identifier)
	assert.Equal(t, institution, intelObj.Institution)
	assert.Equal(t, institutionId, intelObj.InstitutionID)
	assert.Equal(t, sourceOrganization, intelObj.SourceOrganization)
	assert.Equal(t, constants.StateActive, intelObj.State)
	assert.Equal(t, constants.StorageStandard, intelObj.StorageOption)
	assert.Equal(t, title, intelObj.Title)

	// Make sure we use BestAvailableDescription

	aptDescTag := obj.GetTag("aptrust-info.txt", "Description")
	aptDescTag.Value = ""
	intelObj = obj.ToIntellectualObject()
	assert.Equal(t, internalDescription, intelObj.Description)

	intDescTag := obj.GetTag("bag-info.txt", "Internal-Sender-Description")
	intDescTag.Value = ""
	intelObj = obj.ToIntellectualObject()
	assert.Equal(t, externalDescription, intelObj.Description)
}

func TestNewObjectCreationEvent(t *testing.T) {
	obj := getObjectWithTags()
	event := obj.NewObjectCreationEvent()
	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventCreation, event.EventType)
	assert.False(t, event.DateTime.IsZero())
	assert.Equal(t, "Object created", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "Intellectual object created", event.OutcomeDetail)
	assert.Equal(t, "APTrust preservation services", event.Object)
	assert.Equal(t, "test.edu/some-bag", event.IntellectualObjectIdentifier)
	assert.Equal(t, "https://github.com/APTrust/preservation-services", event.Agent)
	assert.Equal(t, "Object created, files copied to preservation storage", event.OutcomeInformation)
}

func TestNewObjectIngestEvent(t *testing.T) {
	obj := getObjectWithTags()
	event := obj.NewObjectIngestEvent()
	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventIngestion, event.EventType)
	assert.False(t, event.DateTime.IsZero())
	assert.Equal(t, "Copied files to perservation storage", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "12 files copied", event.OutcomeDetail)
	assert.Equal(t, "Minio S3 client", event.Object)
	assert.Equal(t, "test.edu/some-bag", event.IntellectualObjectIdentifier)
	assert.Equal(t, "https://github.com/minio/minio-go", event.Agent)
	assert.Equal(t, "Multipart put using s3 etags", event.OutcomeInformation)
}

func TestNewObjectIdentifierEvent(t *testing.T) {
	obj := getObjectWithTags()
	event := obj.NewObjectIdentifierEvent()
	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventIdentifierAssignment, event.EventType)
	assert.False(t, event.DateTime.IsZero())
	assert.Equal(t, "Assigned object identifier test.edu/some-bag", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "test.edu/some-bag", event.OutcomeDetail)
	assert.Equal(t, "APTrust preservation services", event.Object)
	assert.Equal(t, "test.edu/some-bag", event.IntellectualObjectIdentifier)
	assert.Equal(t, "https://github.com/APTrust/preservation-services", event.Agent)
	assert.Equal(t, "Institution domain + tar file name", event.OutcomeInformation)
}

func TestNewObjectRightsEvent(t *testing.T) {
	obj := getObjectWithTags()
	event := obj.NewObjectRightsEvent()
	assert.True(t, util.LooksLikeUUID(event.Identifier))
	assert.Equal(t, constants.EventAccessAssignment, event.EventType)
	assert.False(t, event.DateTime.IsZero())
	assert.Equal(t, "Assigned object access rights", event.Detail)
	assert.Equal(t, constants.StatusSuccess, event.Outcome)
	assert.Equal(t, "consortia", event.OutcomeDetail)
	assert.Equal(t, "APTrust preservation services", event.Object)
	assert.Equal(t, "test.edu/some-bag", event.IntellectualObjectIdentifier)
	assert.Equal(t, "https://github.com/APTrust/preservation-services", event.Agent)
	assert.Equal(t, "Set access to consortia", event.OutcomeInformation)
}

const IngestObjectJson = `{"copied_to_staging_at":"0001-01-01T00:00:00Z","deleted_from_receiving_at":"1904-06-16T15:04:05Z","etag":"12345678","error_message":"No error","file_count":0,"has_fetch_txt":false,"id":555,"institution":"test.edu","institution_id":9855,"is_reingest":false,"manifests":["manifest-md5.txt","manifest-sha256.txt"],"parsable_tag_files":["bag-info.txt","aptrust-info.txt"],"s3_bucket":"aptrust.receiving.test.edu","s3_key":"some-bag.tar","saved_to_registry_at":"0001-01-01T00:00:00Z","serialization":"application/tar","should_delete_from_receiving":false,"size":99999,"storage_option":"Standard","tag_files":["bag-info.txt","aptrust-info.txt","misc/custom-tag-file.txt"],"tag_manifests":["tagmanifest-md5.txt","tagmanifest-sha256.txt"],"tags":[]}`
