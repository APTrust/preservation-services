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
	"github.com/APTrust/preservation-services/util"
	uuid "github.com/satori/go.uuid"
)

// IngestObject contains information about an object being ingested.
type IngestObject struct {

	// CopiedToStagingAt describes when an object's files were
	// copied to the S3 staging bucket.
	CopiedToStagingAt time.Time `json:"copied_to_staging_at,omitempty"`

	// DeletedFromReceivingAt describes when an object's original
	// tar file was deleted from the depositor's receiving bucket.
	DeletedFromReceivingAt time.Time `json:"deleted_from_receiving_at,omitempty"`

	// ETag is the etag of the object's original tarred bag file
	// in the depositor's receiving bucket. We need to track this
	// during ingest because sometimes depositors upload a newer
	// version of a bag while we're still ingesting the older version.
	ETag string `json:"etag,omitempty"`

	// ErrorMessage is an error message describing what went wrong during
	// ingest. TODO: Delete this if it's obsolete.
	ErrorMessage string `json:"error_message,omitempty"`

	// FileCount is the total number of files in the original tarred bag.
	// Note that this will always be a little higher than the number of
	// payload files because it includes bagit.txt, manifests, and tag
	// manifests, none of which we preserve.
	FileCount int `json:"file_count"`

	// HasFetchTxt indicates whether or not the bag has a fetch.txt file.
	// Many BagIt profiles, including APTrust and BTR, prohibit this file,
	// and its presence automatically invalidates the bag.
	HasFetchTxt bool `json:"has_fetch_txt"`

	// ID is the ID of an IntellectualObject in Pharos. This will be zero
	// for objects that have never before been ingested (which is 99% of
	// the bags depositors upload). If we're ingesting a newer version of
	// an existing object, this ID will be set to the ID of the original
	// object early in the ingest process so that we update the right records
	// in Pharos when ingest is complete. If we're ingesting a new bag, this
	// ID will be set to the ID of the newly saved IntellectualObject, and
	// ingest services will be sure to stamp any new GenericFiles with this
	// object ID before saving them to Pharos.
	ID int `json:"id,omitempty"`

	// Institution is the identifier of the institution that is depositing
	// this object. E.g. "test.edu", "virginia.edu", etc.
	Institution string `json:"institution,omitempty"`

	// InstitutionID is the ID of the institution depositing this object.
	InstitutionID int `json:"institution_id,omitempty"`

	// IsReingest will be set to true by the reingest manager if the bag
	// being ingested is an update to an existing bag.
	IsReingest bool `json:"is_reingest"`

	// Manifests contains a list of all the manifests found in the bag.
	// Each value is the relative path within the bag of the manifest.
	// E.g. "manifest-md5.txt", "manifest-256.txt"
	Manifests []string `json:"manifests"`

	// ParsableTagFiles contains a list of the relative paths of all tag
	// files within the bag that appear to be parsable bagit tag files.
	// E.g. "bagit.txt", "bag-info.txt", etc.
	ParsableTagFiles []string `json:"parsable_tag_files"`

	// PremisEvents contains a list of Premis Events that will need to be
	// recorded for this ingest. Do not write to this list directly.
	// Use GetIngestEvents() instead. This list is public so it can be
	// serialized to JSON and persisted to Redis. We need to maintain a
	// single list of Premis Events for each ingest, with one fixed set
	// of UUIDs. If we regenerate this list between retries, each event
	// will get a new UUID and will be recorded as a new event in Pharos,
	// even when it shouldn't be. So we generate the list once, late in
	// the ingest process, and keep it in Redis in case we have to retry
	// the Pharos data recording step.
	PremisEvents []*registry.PremisEvent `json:"premis_events,omitempty"`

	// S3Bucket is the name of the S3 bucket to which the depositor uploaded
	// this object (tarred bag) for ingest.
	S3Bucket string `json:"s3_bucket,omitempty"`

	// S3Key is the name of the bag in the S3Bucket. E.g. "test.edu/mybag.tar".
	S3Key string `json:"s3_key,omitempty"`

	// SavedToRegistryAt is a timestamp describing when this object's ingest
	// data was saved to Pharos.
	SavedToRegistryAt time.Time `json:"saved_to_registry_at,omitempty"`

	// Serialization describes if and how this bag was serialized in the
	// receving bucket. It should always be ".tar", since we currently
	// don't accept zip, gzip, or unserialized bags.
	Serialization string `json:"serialization,omitempty"`

	// ShouldDeleteFromReceiving describes whether this object's
	// original tar file should be deleted from receiving. This should
	// be true in only two cases: 1) the item was successfully ingested,
	// or 2) the bag is invalid and cannot be ingested.
	ShouldDeleteFromReceiving bool `json:"should_delete_from_receiving"`

	// Size is the size of the tarred bag file in the receiving bucket.
	// This will always be larger than the total size of the payload,
	// because tar headers, tag files, and manifests add some bulk to
	// the tar file.
	Size int64 `json:"size,omitempty"`

	// StorageOption is the storage option the depositor has chosen for
	// this bag. The list of allowed storage option values is at
	// https://aptrust.github.io/userguide/bagging/#allowed-storage-option-values
	//
	// This option is specified by the depositor in the aptrust-info.txt
	// file and defaults to "Standard" if not specified. (In APTrust's early
	// years, "Standard" was the only option, so there was no Storage-Option
	// tag. Depositors' bagging processes produced bags without this tag,
	// which we still accept.)
	//
	// Note that the ingest process may change the value of StorageOption to
	// match the storage option of the existing object in Pharos. This behavior
	// is documented on the storage options page (url above). We do this to
	// avoid having multiple inconsistent versions of a file in different
	// storage locations.
	StorageOption string `json:"storage_option"`

	// TagFiles is a list of the relative paths of all tag files found in
	// the bag. This includes both parsable and non-parsable tag files.
	// Note that any file that is not a payload file or manifest is considered
	// a tag file.
	TagFiles []string `json:"tag_files"`

	// TagManifests contains a list of all the tag manifests found in the
	// bag. Each value is the relative path within the bag of the manifest.
	// E.g. "tagmanifest-md5.txt", "tagmanifest-256.txt"
	TagManifests []string `json:"tag_manifests"`

	// Tags contains a list of tag objects parsed from key-value pair
	// files like bagit.txt, bag-info.txt, and aptrust-info.txt.
	Tags []*bagit.Tag `json:"tags"`
}

// NewIngestObject creates a new IngestObject. The S3Bucket and S3Key describe
// where the tarred bag can be found. eTag is the bag's eTag. The institution
// and institutionID describe who owns the bag. The size is the size of the
// tarred bag in the receiving bucket.
func NewIngestObject(s3Bucket, s3Key, eTag, institution string, institutionID int, size int64) *IngestObject {
	return &IngestObject{
		ETag:                      strings.Replace(eTag, "\"", "", -1),
		HasFetchTxt:               false,
		Institution:               institution,
		InstitutionID:             institutionID,
		IsReingest:                false,
		Manifests:                 make([]string, 0),
		ParsableTagFiles:          make([]string, 0),
		PremisEvents:              make([]*registry.PremisEvent, 0),
		S3Bucket:                  s3Bucket,
		S3Key:                     s3Key,
		Size:                      size,
		ShouldDeleteFromReceiving: false,
		StorageOption:             constants.StorageStandard,
		TagFiles:                  make([]string, 0),
		TagManifests:              make([]string, 0),
		Tags:                      make([]*bagit.Tag, 0),
	}
}

// IngestObjectFromJSON converts the JSON representation of an IngestObject
// to an actual object.
func IngestObjectFromJSON(jsonData string) (*IngestObject, error) {
	obj := &IngestObject{}
	err := json.Unmarshal([]byte(jsonData), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// ToJSON converts this object to its JSON representation.
func (obj *IngestObject) ToJSON() (string, error) {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// BagName returns the bag's name of the tar file, minus the .tar suffix
// and the ".bagN.ofN" coponent.
func (obj *IngestObject) BagName() string {
	return bagit.CleanBagName(obj.S3Key)
}

// BaseNameOfS3Key returns the name of the S3 Key, minus the file extension.
func (obj *IngestObject) BaseNameOfS3Key() string {
	ext := path.Ext(obj.S3Key)
	re := regexp.MustCompile("\\" + ext + "$")
	return re.ReplaceAllString(obj.S3Key, "")
}

// Identifier is the IntellectualObject.Identifier of this bag, which
// has the format <institution_identifier>/<bag_name>.
func (obj *IngestObject) Identifier() string {
	return fmt.Sprintf("%s/%s", obj.Institution, obj.BagName())
}

// FileIdentifier returns a new file identifier for this bag. Param
// filename should be the relative path of the file within the bag.
// For example, if this object is "test.edu/my_bag" and param filename
// is "data/photos/portrait.jpg", this returns
// "test.edu/my_bag/data/photos/portrait.jpg"
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

// GetTag returns first instance of tag in specified file with
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

// BagItProfileFormat returns a string indicating whether the bag
// being ingested is in APTrust or BTR format.
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

// AltIdentifier returns the object's Alternate Identifier.
func (obj *IngestObject) AltIdentifier() string {
	return obj.GetTagValue("bag-info.txt", "Internal-Sender-Identifier", "")
}

// BagGroupIdentifier returns the object's Bag-Group-Identifier.
func (obj *IngestObject) BagGroupIdentifier() string {
	return obj.GetTagValue("bag-info.txt", "Bag-Group-Identifier", "")
}

// BagItProfileIdentifier returns the object's BagIt-Profile-Identifier.
func (obj *IngestObject) BagItProfileIdentifier() string {
	return obj.GetTagValue("bag-info.txt", "BagIt-Profile-Identifier", constants.DefaultProfileIdentifier)
}

// Description returns the Description from the aptrust-info.txt file.
// This may be empty for some APTrust bags, and will always be empty for
// BTR bags. See also ExternalDescription and InternalSenderDescription.
func (obj *IngestObject) Description() string {
	return obj.GetTagValue("aptrust-info.txt", "Description", "")
}

// ExternalDescription returns the External-Description from the
// bag-info.txt file. See also InternalSenderDescription.
func (obj *IngestObject) ExternalDescription() string {
	return obj.GetTagValue("bag-info.txt", "External-Description", "")
}

// ExternalIdentifier returns the External-Identifier from the
// bag-info.txt file.
func (obj *IngestObject) ExternalIdentifier() string {
	return obj.GetTagValue("bag-info.txt", "External-Identifier", "")
}

// InternalIdentifier returns the Internal-Identifier from the
// bag-info.txt file.
func (obj *IngestObject) InternalIdentifier() string {
	return obj.GetTagValue("bag-info.txt", "Internal-Identifier", "")
}

// InternalSenderDescription returns the Internal-Sender-Description from the
// bag-info.txt file. See also ExternalDescription.
func (obj *IngestObject) InternalSenderDescription() string {
	return obj.GetTagValue("bag-info.txt", "Internal-Sender-Description", "")
}

// SourceOrganization returns the Source-Organization from the
// bag-info.txt file.
func (obj *IngestObject) SourceOrganization() string {
	return obj.GetTagValue("bag-info.txt", "Source-Organization", "")
}

// Title returns the value of the Title tag from the aptrust-info.txt file.
// This will return empty for BTR bags.
func (obj *IngestObject) Title() string {
	title := obj.GetTagValue("aptrust-info.txt", "Title", "")
	if title == "" {
		title = obj.InternalIdentifier()
	}
	if title == "" {
		title = obj.ExternalIdentifier()
	}
	if title == "" {
		title = util.StripFileExtension(obj.S3Key)
	}
	return title
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

// ToIntellectualObject returns an IntellectualObject version of this
// IngestObject. Pharos understands the IntellectualObject, but not
// the IngestObject, so we have to convert before we can save data to
// Pharos.
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

// NewObjectCreationEvent returns a new Premis Event describing the
// creation of a new IntellectualObject. We generate this event the first
// time an object is ingested, but not when it is updated.
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

// NewObjectIngestEvent returns a new Premis Event describing the
// ingest of this IntellectualObject. We generate this event each time
// an object is ingested.
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

// NewObjectIdentifierEvent returns a new Premis Event describing the
// assignment of this object's identifier. We generate this event the first
// time an object is ingested, but not when it is updated.
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

// NewObjectRightsEvent returns a new Premis Event describing the
// assignment of this object's access rights. This event is generated
// on initial ingest, and on any re-ingest when access rights change.
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
