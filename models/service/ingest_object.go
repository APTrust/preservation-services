package service

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"path"
	"regexp"
	"strings"
	"time"
)

type IngestObject struct {
	CopiedToStagingAt      time.Time    `json:"copied_to_staging_at,omitempty"`
	DeletedFromReceivingAt time.Time    `json:"deleted_from_receiving_at,omitempty"`
	ETag                   string       `json:"etag,omitempty"`
	ErrorMessage           string       `json:"error_message,omitempty"`
	FileCount              int          `json:"file_count"`
	HasFetchTxt            bool         `json:"has_fetch_txt"`
	Id                     int          `json:"id,omitempty"`
	Institution            string       `json:"institution,omitempty"`
	InstitutionId          int          `json:"institution_id,omitempty"`
	Manifests              []string     `json:"manifests"`
	ParsableTagFiles       []string     `json:"parsable_tag_files"`
	S3Bucket               string       `json:"s3_bucket,omitempty"`
	S3Key                  string       `json:"s3_key,omitempty"`
	Serialization          string       `json:"serialization,omitempty"`
	Size                   int64        `json:"size,omitempty"`
	StorageOption          string       `json:"storage_option"`
	TagFiles               []string     `json:"tag_files"`
	TagManifests           []string     `json:"tag_manifests"`
	Tags                   []*bagit.Tag `json:"tags"`
}

func NewIngestObject(s3Bucket, s3Key, eTag, institution string, institutionId int, size int64) *IngestObject {
	return &IngestObject{
		ETag:             strings.Replace(eTag, "\"", "", -1),
		HasFetchTxt:      false,
		Institution:      institution,
		InstitutionId:    institutionId,
		Manifests:        make([]string, 0),
		ParsableTagFiles: make([]string, 0),
		S3Bucket:         s3Bucket,
		S3Key:            s3Key,
		Size:             size,
		StorageOption:    constants.StorageStandard,
		TagFiles:         make([]string, 0),
		TagManifests:     make([]string, 0),
		Tags:             make([]*bagit.Tag, 0),
	}
}

func IngestObjectFromJson(jsonData string) (*IngestObject, error) {
	obj := &IngestObject{}
	err := json.Unmarshal([]byte(jsonData), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (obj *IngestObject) ToJson() (string, error) {
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
		Access:                 obj.Access(),
		AltIdentifier:          obj.AltIdentifier(),
		BagGroupIdentifier:     obj.BagGroupIdentifier(),
		BagItProfileIdentifier: obj.BagItProfileIdentifier(),
		BagName:                obj.BagName(),
		Description:            obj.BestAvailableDescription(),
		ETag:                   obj.ETag,
		Id:                     obj.Id,
		Identifier:             obj.Identifier(),
		Institution:            obj.Institution,
		InstitutionId:          obj.InstitutionId,
		SourceOrganization:     obj.SourceOrganization(),
		State:                  constants.StateActive,
		StorageOption:          obj.StorageOption,
		Title:                  obj.Title(),
	}
}
