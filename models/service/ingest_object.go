package service

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/preservation-services/models/bagit"
	"strings"
	"time"
)

type IngestObject struct {
	DeletedFromReceivingAt time.Time `json:"deleted_from_receiving_at,omitempty"`
	ETag                   string    `json:"etag,omitempty"`
	ErrorMessage           string    `json:"error_message,omitempty"`
	Id                     int       `json:"id,omitempty"`
	Identifier             string    `json:"identifier,omitempty"`
	Institution            string    `json:"institution,omitempty"`
	Manifests              []string  `json:"manifests"`
	ParsableTagFiles       []string  `json:"parsable_tag_files"`
	S3Bucket               string    `json:"s3_bucket,omitempty"`
	S3Key                  string    `json:"s3_key,omitempty"`
	Size                   int64     `json:"size,omitempty"`
	StorageOption          string    `json:"storage_option"`
	TagManifests           []string  `json:"tag_manifests"`
	TopLevelDirs           []string  `json:"top_level_dirs"`
}

func NewIngestObject(s3Bucket, s3Key, eTag, institution string, size int64) *IngestObject {
	bagName := bagit.CleanBagName(s3Key)
	return &IngestObject{
		ETag:             strings.Replace(eTag, "\"", "", -1),
		Identifier:       fmt.Sprintf("%s/%s", institution, bagName),
		Institution:      institution,
		Manifests:        make([]string, 0),
		ParsableTagFiles: make([]string, 0),
		S3Bucket:         s3Bucket,
		S3Key:            s3Key,
		Size:             size,
		TagManifests:     make([]string, 0),
		TopLevelDirs:     make([]string, 0),
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
