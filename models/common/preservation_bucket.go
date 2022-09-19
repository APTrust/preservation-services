package common

import (
	"fmt"
	"strings"
)

type PreservationBucket struct {
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
func (b *PreservationBucket) URLFor(key string) string {
	return fmt.Sprintf("https://%s/%s/%s", b.GetHostNameWithRegion(), b.Bucket, key)
}

// HostsURL returns true if the given URL is hosted by this PreservationBucket.
func (b *PreservationBucket) HostsURL(url string) bool {
	// Wasabi host names already include region.
	// AWS host names don't (in our system).
	return strings.HasPrefix(url, fmt.Sprintf("https://%s/%s/", b.GetHostNameWithRegion(), b.Bucket))
}

func (b *PreservationBucket) GetHostNameWithRegion() string {
	host := strings.ToLower(b.Host)
	if strings.HasPrefix(host, "s3.") && !b.RegionIsEmbeddedInHostName() {
		host = strings.Replace(host, "s3.", "", 1)
	}
	if !b.RegionIsEmbeddedInHostName() {
		host = fmt.Sprintf("s3.%s.%s", b.Region, host)
	}
	return host
}

func (b *PreservationBucket) RegionIsEmbeddedInHostName() bool {
	return strings.Contains(b.Host, b.Region)
}
