package common

import (
	"fmt"
	"regexp"
	"strings"
)

var HostWithRegionPrefix = regexp.MustCompile("^[Ss]3\\.\\w{2}-\\w+-\\d\\.")

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
	// Wasabi urls include region.
	// Older AWS urls do not include region; newer ones do.
	urlWithRegion := fmt.Sprintf("https://%s/%s/", b.GetHostNameWithRegion(), b.Bucket)
	urlWithoutRegion := fmt.Sprintf("https://%s/%s/", b.Host, b.Bucket)
	return strings.HasPrefix(url, urlWithRegion) || strings.HasPrefix(url, urlWithoutRegion)
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
	return HostWithRegionPrefix.MatchString(b.Host)
}
