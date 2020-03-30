package common

import (
	"fmt"
)

type UploadTarget struct {
	Bucket       string
	Description  string
	Host         string
	OptionName   string
	Provider     string
	Region       string
	StorageClass string
}

// URLFor returns the URL for the specified key. For example:
// target.URLFor(uuid) returns something like
// https://s3.us-east-1.amazonaws.com/aptrust.preservation.storage/uuid
// for an AWS upload target, or
// https://s3.us-west-1.wasabisys.com/aptrust.wasabi.or/
// for a Wasabi target.
func (target *UploadTarget) URLFor(key string) string {
	return fmt.Sprintf("https://s3.%s.%s/%s/%s",
		target.Region, target.Host, target.Bucket, key)
}
