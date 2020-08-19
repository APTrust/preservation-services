package common

import (
	"fmt"
	"strings"
)

type PerservationBucket struct {
	Bucket      string
	Description string
	Host        string
	OptionName  string
	Provider    string
	Region      string

	// RestorePriority describes the best preservation bucket to restore
	// from. For restorations, we always choose S3 storage over Glacier,
	// and then we try to choose preservation buckets closest to Virginia.
	RestorePriority int
	StorageClass    string
}

// URLFor returns the URL for the specified key. For example:
// target.URLFor(uuid) returns something like
// https://s3.us-east-1.amazonaws.com/aptrust.preservation.storage/uuid
// for an AWS upload target, or
// https://s3.us-west-1.wasabisys.com/aptrust.wasabi.or/
// for a Wasabi target.
func (target *PerservationBucket) URLFor(key string) string {
	return fmt.Sprintf("https://s3.%s.%s/%s/%s",
		target.Region, target.Host, target.Bucket, key)
}

// HostsURL returns true if the given URL is hosted by this upload target.
func (target *PerservationBucket) HostsURL(url string) bool {
	return strings.HasPrefix(url, fmt.Sprintf("https://s3.%s.%s/%s/", target.Region, target.Host, target.Bucket))
}
