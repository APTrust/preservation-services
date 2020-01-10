package util

import (
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"strings"
)

// StringListContains returns true if the list of strings contains item.
func StringListContains(list []string, item string) bool {
	if list != nil {
		for i := range list {
			if list[i] == item {
				return true
			}
		}
	}
	return false
}

// GetAlgFromManifestName returns the algorithm used in a tag manifest.
// For example, arg "manifest-sha256.txt" returns "sha256", while
// "tagmanifest-sha512.txt" returns "sha512". This returns an error if
// it can't find the algorithm in the manifest name.
func GetAlgFromManifestName(manifestName string) (string, error) {
	for _, alg := range constants.DigestAlgorithms {
		if strings.Contains(manifestName, alg) {
			return alg, nil
		}
	}
	return "", fmt.Errorf("Can't parse algorithm from filename %s", manifestName)
}

func LooksLikeManifest(name string) bool {
	return strings.HasPrefix(name, "manifest-") && strings.HasSuffix(name, ".txt")
}

func LooksLikeTagManifest(name string) bool {
	return strings.HasPrefix(name, "tagmanifest-") && strings.HasSuffix(name, ".txt")
}
