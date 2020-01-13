package util

import (
	"fmt"
	"regexp"
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

// StringListContainsAll returns true if all items in listToCheck are also
// in masterList. Be sure you pass the params in the right order. Note
// that this can get expensive if the lists are long.
func StringListContainsAll(masterList []string, listToCheck []string) bool {
	for _, item := range listToCheck {
		if !StringListContains(masterList, item) {
			return false
		}
	}
	return true
}

func LooksLikeManifest(name string) bool {
	return strings.HasPrefix(name, "manifest-") && strings.HasSuffix(name, ".txt")
}

func LooksLikeTagManifest(name string) bool {
	return strings.HasPrefix(name, "tagmanifest-") && strings.HasSuffix(name, ".txt")
}

func AlgorithmFromManifestName(filename string) (string, error) {
	re := regexp.MustCompile(`manifest-(?P<Alg>[^\.]+).txt$`)
	match := re.FindStringSubmatch(filename)
	if len(match) > 1 {
		return match[1], nil
	}
	return "", fmt.Errorf("Cannot get algorithm from filename %s", filename)
}
