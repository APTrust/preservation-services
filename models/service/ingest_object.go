package service

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/constants"
	"strings"
	"time"
)

type IngestObject struct {
	DeletedFromReceivingAt time.Time    `json:"deleted_from_receiving_at,omitempty"`
	ETag                   string       `json:"etag,omitempty"`
	ErrorMessage           string       `json:"error_message,omitempty"`
	Id                     int          `json:"id,omitempty"`
	Institution            string       `json:"institution,omitempty"`
	Manifests              []string     `json:"manifests"`
	ParsableTagFiles       []string     `json:"parsable_tag_files"`
	S3Bucket               string       `json:"s3_bucket,omitempty"`
	S3Key                  string       `json:"s3_key,omitempty"`
	Size                   int64        `json:"size,omitempty"`
	StorageOption          string       `json:"storage_option"`
	TagManifests           []string     `json:"tag_manifests"`
	Tags                   []*bagit.Tag `json:"tags"`
}

func NewIngestObject(s3Bucket, s3Key, eTag, institution string, size int64) *IngestObject {
	return &IngestObject{
		ETag:             strings.Replace(eTag, "\"", "", -1),
		Institution:      institution,
		Manifests:        make([]string, 0),
		ParsableTagFiles: make([]string, 0),
		S3Bucket:         s3Bucket,
		S3Key:            s3Key,
		Size:             size,
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

func (obj *IngestObject) Identifier() string {
	return fmt.Sprintf("%s/%s", obj.Institution, obj.BagName())
}

func (obj *IngestObject) FileIdentifier(filename string) string {
	return fmt.Sprintf("%s/%s/%s", obj.Institution, obj.BagName(), filename)
}

func (obj *IngestObject) GetTags(tagFile, tagName string) []*bagit.Tag {
	tags := make([]*bagit.Tag, 0)
	for _, tag := range obj.Tags {
		if tag.SourceFile == tagFile && tag.Label == tagName {
			tags = append(tags, tag)
		}
	}
	return tags
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
