package service

import (
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	uuid "github.com/satori/go.uuid"
)

type IngestObject struct {
	CopiedToStagingAt      time.Time               `json:"copied_to_staging_at,omitempty"`
	DeletedFromReceivingAt time.Time               `json:"deleted_from_receiving_at,omitempty"`
	ETag                   string                  `json:"etag,omitempty"`
	ErrorMessage           string                  `json:"error_message,omitempty"`
	FileCount              int                     `json:"file_count"`
	HasFetchTxt            bool                    `json:"has_fetch_txt"`
	ID                     int                     `json:"id,omitempty"`
	Institution            string                  `json:"institution,omitempty"`
	InstitutionID          int                     `json:"institution_id,omitempty"`
	IsReingest             bool                    `json:"is_reingest"`
	Manifests              []string                `json:"manifests"`
	ParsableTagFiles       []string                `json:"parsable_tag_files"`
	PremisEvents           []*registry.PremisEvent `json:"premis_events,omitempty"`
	S3Bucket               string                  `json:"s3_bucket,omitempty"`
	S3Key                  string                  `json:"s3_key,omitempty"`
	SavedToRegistryAt      time.Time               `json:"saved_to_registry_at,omitempty"`
	Serialization          string                  `json:"serialization,omitempty"`
	Size                   int64                   `json:"size,omitempty"`
	StorageOption          string                  `json:"storage_option"`
	TagFiles               []string                `json:"tag_files"`
	TagManifests           []string                `json:"tag_manifests"`
	Tags                   []*bagit.Tag            `json:"tags"`
}

func NewIngestObject(s3Bucket, s3Key, eTag, institution string, institutionID int, size int64) *IngestObject {
	return &IngestObject{
		ETag:             strings.Replace(eTag, "\"", "", -1),
		HasFetchTxt:      false,
		Institution:      institution,
		InstitutionID:    institutionID,
		IsReingest:       false,
		Manifests:        make([]string, 0),
		ParsableTagFiles: make([]string, 0),
		PremisEvents:     make([]*registry.PremisEvent, 0),
		S3Bucket:         s3Bucket,
		S3Key:            s3Key,
		Size:             size,
		StorageOption:    constants.StorageStandard,
		TagFiles:         make([]string, 0),
		TagManifests:     make([]string, 0),
		Tags:             make([]*bagit.Tag, 0),
	}
}

func IngestObjectFromJSON(jsonData string) (*IngestObject, error) {
	obj := &IngestObject{}
	err := json.Unmarshal([]byte(jsonData), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (obj *IngestObject) ToJSON() (string, error) {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func (obj *IngestObject) BagName() string {
	return bagit.CleanBagName(obj.S3Key)
}

func (obj *IngestObject) BaseNameOfS3Key() string {
	ext := path.Ext(obj.S3Key)
	re := regexp.MustCompile("\\" + ext + "$")
	return re.ReplaceAllString(obj.S3Key, "")
}

func (obj *IngestObject) Identifier() string {
	return fmt.Sprintf("%s/%s", obj.Institution, obj.BagName())
}

func (obj *IngestObject) FileIdentifier(filename string) string {
	return fmt.Sprintf("%s/%s/%s", obj.Institution, obj.BagName(), filename)
}

// GetTags returns all instances of tags in specified file with
// specified name.
func (obj *IngestObject) GetTags(tagFile, tagName string) []*bagit.Tag {
	tags := make([]*bagit.Tag, 0)
	for _, tag := range obj.Tags {
		if tag.TagFile == tagFile && tag.TagName == tagName {
			tags = append(tags, tag)
		}
	}
	return tags
}

// GetTags returns first instance of tag in specified file with
// specified name.
func (obj *IngestObject) GetTag(tagFile, tagName string) *bagit.Tag {
	var tag *bagit.Tag
	tags := obj.GetTags(tagFile, tagName)
	if len(tags) > 0 {
		tag = tags[0]
	}
	return tag
}

// GetTagValue returns the first tag value in tagName in tagFile,
// or defaultValue if no matching tag is found.
func (obj *IngestObject) GetTagValue(tagFile, tagName, defaultValue string) string {
	value := defaultValue
	tag := obj.GetTag(tagFile, tagName)
	if tag != nil && tag.Value != "" {
		value = tag.Value
	}
	return value
}

func (obj *IngestObject) BagItProfileFormat() string {
	profile := constants.BagItProfileDefault
	profileIdentifier := ""
	tags := obj.GetTags("bag-info.txt", "BagIt-Profile-Identifier")
	if len(tags) > 0 {
		profileIdentifier = tags[0].Value
	}
	if strings.Contains(profileIdentifier, "btr-bagit-profile") {
		profile = constants.BagItProfileBTR
	}
	return profile
}

// Access returns the value of the aptrust-info.txt/Access tag, or
// constants.DefaultAccess if that tag isn't present. (Bags submitted
// in BTR BagIt format will not have an Access tag.) Note the return
// value will match one of the constants.Access* values, and should
// begin with an upper case letter.
func (obj *IngestObject) Access() string {
	access := obj.GetTagValue("aptrust-info.txt", "Access", constants.DefaultAccess)
	return strings.ToLower(access) // util.UCFirst(access)
}

func (obj *IngestObject) AltIdentifier() string {
	return obj.GetTagValue("bag-info.txt", "Internal-Sender-Identifier", "")
}

func (obj *IngestObject) BagGroupIdentifier() string {
	return obj.GetTagValue("bag-info.txt", "Bag-Group-Identifier", "")
}

func (obj *IngestObject) BagItProfileIdentifier() string {
	return obj.GetTagValue("bag-info.txt", "BagIt-Profile-Identifier", constants.DefaultProfileIdentifier)
}

func (obj *IngestObject) Description() string {
	return obj.GetTagValue("aptrust-info.txt", "Description", "")
}

func (obj *IngestObject) ExternalDescription() string {
	return obj.GetTagValue("bag-info.txt", "External-Description", "")
}

func (obj *IngestObject) ExternalIdentifier() string {
	return obj.GetTagValue("bag-info.txt", "External-Identifier", "")
}

func (obj *IngestObject) InternalSenderDescription() string {
	return obj.GetTagValue("bag-info.txt", "Internal-Sender-Description", "")
}

func (obj *IngestObject) SourceOrganization() string {
	return obj.GetTagValue("bag-info.txt", "Source-Organization", "")
}

func (obj *IngestObject) Title() string {
	return obj.GetTagValue("aptrust-info.txt", "Title", "")
}

// Returns the best available description, which is the first non-empty one
// of aptrust-info.txt/Description, bag-info.txt/Internal-Sender-Description,
// and bag-info.txt/External-Description in that order.
func (obj *IngestObject) BestAvailableDescription() string {
	desc := obj.Description()
	if desc == "" {
		desc = obj.InternalSenderDescription()
	}
	if desc == "" {
		desc = obj.ExternalDescription()
	}
	return desc
}

func (obj *IngestObject) ToIntellectualObject() *registry.IntellectualObject {
	return &registry.IntellectualObject{
		Access:                    obj.Access(),
		AltIdentifier:             obj.AltIdentifier(),
		BagGroupIdentifier:        obj.BagGroupIdentifier(),
		BagItProfileIdentifier:    obj.BagItProfileIdentifier(),
		BagName:                   obj.BagName(),
		Description:               obj.BestAvailableDescription(),
		ETag:                      obj.ETag,
		ID:                        obj.ID,
		Identifier:                obj.Identifier(),
		Institution:               obj.Institution,
		InstitutionID:             obj.InstitutionID,
		InternalSenderDescription: obj.InternalSenderDescription(),
		InternalSenderIdentifier:  obj.AltIdentifier(),
		SourceOrganization:        obj.SourceOrganization(),
		State:                     constants.StateActive,
		StorageOption:             obj.StorageOption,
		Title:                     obj.Title(),
	}
}

// GetIngestEvents returns this object's list of ingest PremisEvents.
// It generates the list if the list does not already exist.
//
// Note that this list should be generated only once, and the events
// should be preserved in Redis so that if any part of registry data
// recording process fails, we can retry and know that we are not
// creating new PremisEvents in Pharos. When Pharos sees these event
// UUIDs already exist, it will not create duplicate entries. If we
// don't persist events with their UUIDs in Redis intermediate storage,
// we will be sending new events with new UUIDs each time we retry
// the ingest recording process, and we'll have lots of duplicate
// events in our registry.
func (obj *IngestObject) GetIngestEvents() []*registry.PremisEvent {
	if obj.PremisEvents == nil {
		obj.PremisEvents = make([]*registry.PremisEvent, 0)
	}
	if len(obj.PremisEvents) == 0 {
		obj.initIngestEvents()
	}
	return obj.PremisEvents
}

func (obj *IngestObject) initIngestEvents() {
	// Object creation and identifier assignment happen only
	// on first ingest.
	if !obj.IsReingest {
		obj.PremisEvents = append(obj.PremisEvents, obj.NewObjectCreationEvent())
		obj.PremisEvents = append(obj.PremisEvents, obj.NewObjectIdentifierEvent())
	}
	// Ingest event is added for *every* ingest,
	// and rights assignment is updated on each ingest,
	// the usually it doesn't change.
	obj.PremisEvents = append(obj.PremisEvents, obj.NewObjectIngestEvent())
	obj.PremisEvents = append(obj.PremisEvents, obj.NewObjectRightsEvent())
}

func (obj *IngestObject) NewObjectCreationEvent() *registry.PremisEvent {
	eventId := uuid.NewV4()
	timestamp := time.Now().UTC()
	return &registry.PremisEvent{
		Identifier:                   eventId.String(),
		EventType:                    constants.EventCreation,
		DateTime:                     timestamp,
		Detail:                       "Object created",
		Outcome:                      constants.StatusSuccess,
		OutcomeDetail:                "Intellectual object created",
		Object:                       "APTrust preservation services",
		IntellectualObjectIdentifier: obj.Identifier(),
		Agent:                        "https://github.com/APTrust/preservation-services",
		OutcomeInformation:           "Object created, files copied to preservation storage",
		CreatedAt:                    timestamp,
		UpdatedAt:                    timestamp,
	}
}

func (obj *IngestObject) NewObjectIngestEvent() *registry.PremisEvent {
	eventId := uuid.NewV4()
	timestamp := time.Now().UTC()
	return &registry.PremisEvent{
		Identifier:                   eventId.String(),
		EventType:                    constants.EventIngestion,
		DateTime:                     timestamp,
		Detail:                       "Copied files to perservation storage",
		Outcome:                      constants.StatusSuccess,
		OutcomeDetail:                fmt.Sprintf("%d files copied", obj.FileCount),
		Object:                       "Minio S3 client",
		IntellectualObjectIdentifier: obj.Identifier(),
		Agent:                        "https://github.com/minio/minio-go",
		OutcomeInformation:           "Multipart put using s3 etags",
		CreatedAt:                    timestamp,
		UpdatedAt:                    timestamp,
	}
}

func (obj *IngestObject) NewObjectIdentifierEvent() *registry.PremisEvent {
	eventId := uuid.NewV4()
	timestamp := time.Now().UTC()
	return &registry.PremisEvent{
		Identifier:                   eventId.String(),
		EventType:                    constants.EventIdentifierAssignment,
		DateTime:                     timestamp,
		Detail:                       "Assigned object identifier " + obj.Identifier(),
		Outcome:                      constants.StatusSuccess,
		OutcomeDetail:                obj.Identifier(),
		Object:                       "APTrust preservation services",
		IntellectualObjectIdentifier: obj.Identifier(),
		Agent:                        "https://github.com/APTrust/preservation-services",
		OutcomeInformation:           "Institution domain + tar file name",
		CreatedAt:                    timestamp,
		UpdatedAt:                    timestamp,
	}
}

func (obj *IngestObject) NewObjectRightsEvent() *registry.PremisEvent {
	eventId := uuid.NewV4()
	timestamp := time.Now().UTC()
	return &registry.PremisEvent{
		Identifier:                   eventId.String(),
		EventType:                    constants.EventAccessAssignment,
		DateTime:                     timestamp,
		Detail:                       "Assigned object access rights",
		Outcome:                      constants.StatusSuccess,
		OutcomeDetail:                obj.Access(),
		Object:                       "APTrust preservation services",
		IntellectualObjectIdentifier: obj.Identifier(),
		Agent:                        "https://github.com/APTrust/preservation-services",
		OutcomeInformation:           "Set access to " + obj.Access(),
		CreatedAt:                    timestamp,
		UpdatedAt:                    timestamp,
	}
}
