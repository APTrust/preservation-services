package bagit

import (
	"regexp"
)

// The tar files that make up multipart bags include a suffix
// that follows this pattern. For example, after stripping off
// the .tar suffix, you'll have a name like "my_bag.b04.of12"
var MultipartSuffix = regexp.MustCompile("\\.b\\d+\\.of\\d+$")

// Matches strings that end with .tar
var TarSuffix = regexp.MustCompile("\\.tar$")

// CleanBagName returns the clean bag name. That's the tar file name minus
// the tar extension and any ".bagN.ofN" suffix.
func CleanBagName(bagName string) string {
	nameMinusTarSuffix := TarSuffix.ReplaceAllString(bagName, "")
	return MultipartSuffix.ReplaceAllString(nameMinusTarSuffix, "")
}
