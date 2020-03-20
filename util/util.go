package util

import (
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"unicode"
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

// LooksLikeURL returns true if url looks like a URL.
func LooksLikeURL(url string) bool {
	reUrl := regexp.MustCompile(`^(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)*\/?$`)
	return reUrl.Match([]byte(url))
}

// LooksLikeUUID returns true if uuid looks like a valid UUID.
func LooksLikeUUID(uuid string) bool {
	reUUID := regexp.MustCompile(`(?i)^([a-f\d]{8}(-[a-f\d]{4}){3}-[a-f\d]{12}?)$`)
	return reUUID.Match([]byte(uuid))
}

func AlgorithmFromManifestName(filename string) (string, error) {
	re := regexp.MustCompile(`manifest-(?P<Alg>[^\.]+).txt$`)
	match := re.FindStringSubmatch(filename)
	if len(match) > 1 {
		return match[1], nil
	}
	return "", fmt.Errorf("Cannot get algorithm from filename %s", filename)
}

// ContainsControlCharacter returns true if string str contains a
// Unicode control character. We use this to test file names, which
// should not contain control characters.
func ContainsControlCharacter(str string) bool {
	runes := []rune(str)
	for _, _rune := range runes {
		if unicode.IsControl(_rune) {
			return true
		}
	}
	return false
}

// ContainsEscapedControl returns true if string str contains
// something that looks like an escaped UTF-8 control character.
// The Mac OS file system seems to silently escape UTF-8 control
// characters. That causes problems when we try to copy a file
// over to another file system that won't accept the control
// character in a file name. The bag validator looks for file names
// matching these patterns and rejects them.
func ContainsEscapedControl(str string) bool {
	reControl := regexp.MustCompile("\\\\[Uu]00[0189][0-9A-Fa-f]|\\\\[Uu]007[Ff]")
	return reControl.MatchString(str)
}

// UCFirst returns string str with the first letter capitalized
// and all others lower case.
func UCFirst(str string) string {
	return strings.Title(strings.ToLower(str))
}

// TarPathToBagPath, given the path of a file inside a tarball, returns
// the path of the file in a bag. The name param generally comes from
// the Name property of a tar file header. For example, in a tar file
// called my_bag.tar the paths would translate as follows:
//
// Input                      ->  Output
// my_bag/bagit.txt           ->  bagit.txt
// my_bag/data/file.docx      ->  data/file.docx
// my_bag/data/img/photo.jpg  ->  data/img/photo.jpg
//
// This function assumes (perhaps dangerously) that tarred bags follow
// the recommdation of pre-1.0 versions of the BagIt spec that say
// a tarred bag should deserialize to a single top-level directory.
// This function does not assume that the directory will match the
// bag name.
func TarPathToBagPath(name string) (string, error) {
	prefix := strings.Split(name, "/")[0] + "/"
	pathInBag := strings.Replace(name, prefix, "", 1)
	if pathInBag == name {
		return "", fmt.Errorf("Illegal path, '%s'. Should start with '%s'.", name, prefix)
	}
	return pathInBag, nil
}

// PathTo returns the path to the specified program.
func PathTo(program string) (string, error) {
	output, err := exec.Command("which", program).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func StringIsShellSafe(s string) bool {
	unsafeChars := "\"';{}|$` \t\r\n<>"
	return !strings.ContainsAny(s, unsafeChars)
}
