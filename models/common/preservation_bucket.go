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
// preservationBucket.URLFor(uuid) returns something like
// https://s3.us-east-1.amazonaws.com/aptrust.preservation.storage/uuid
// for an AWS upload preservationBucket, or
// https://s3.us-west-1.wasabisys.com/aptrust.wasabi.or/
// for a Wasabi preservationBucket.
func (b *PerservationBucket) URLFor(key string) string {
	// While the general S3 host URLs look like s3.amazonaws.com
	// and s3.wasabisys.com, for specific object URLs, we have to
	// move the s3.prefix to the front of the URL.
	host := strings.Replace(b.Host, "s3.", "", 1)
	return fmt.Sprintf("https://s3.%s.%s/%s/%s", b.Region, host, b.Bucket, key)
}

// HostsURL returns true if the given URL is hosted by this PreservationBucket.
func (b *PerservationBucket) HostsURL(url string) bool {
	host := strings.Replace(b.Host, "s3.", "", 1)
	return strings.HasPrefix(url, fmt.Sprintf("https://s3.%s.%s/%s/", b.Region, host, b.Bucket))
}
