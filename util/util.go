package util

import (
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
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
	reURL := regexp.MustCompile(`^(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)*\/?$`)
	return reURL.Match([]byte(url))
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
//
// This also catches the unicode non-breaking space, \xc2\xa0,
// which Go does not consider a control character, but does cause
// problems because S3 will not accept files or metadata containing
// this character. We hit this case in production in April, 2023.
// See https://trello.com/c/J7Yd4uhg.
func ContainsControlCharacter(str string) bool {
	nbSpace := []rune(" ")[0] // unicode non-breaking space \xc2\xa0
	for _, _rune := range str {
		if unicode.IsControl(_rune) || _rune == nbSpace {
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
//
// For APTrust's first several years of operation, we and depositors
// were using BagIt spec v. 0.97, which explicitly required that all
// tarred bag contents be inside a single folder within the tarball.
// That requirement was dropped in BagIt v 1.0, released in October
// 2018. By then, APTrust had been in operation for nearly four years,
// and we had openly documented this requirement.
//
// It wasn't till Spring of 2024 that we received a handful of bags
// from one depositor in which tarballs did not extract to a single
// directory. We've been rejecting these bags. See https://trello.com/c/548wCyeT.
//
// Our BagIt profile at https://github.com/APTrust/preservation-services/blob/master/profiles/aptrust-v2.3.json
// has for years, and still does say that tarDirMustMatchName = true
// (see the last line of the file). While we have quietly dropped the
// requirement that the top-level directory of the tar file must
// match the name of the tar file, we do still require that there be
// a top-level directory. So, this list of tar file contents would be
// OK:
//
// my_bag.tar
//   - my_bag
//   - my_bag/bag-info.txt
//   - my_bag/aptrust-info.txt
//   - my_bag/data
//   - my_bag/data/file_1.jpg
//   - my_bag/data/file_2.jpg
//
// While this list, with no top-level directory, is NOT OK:
//
// my_bag.tar
//   - bag-info.txt
//   - aptrust-info.txt
//   - data
//   - data/file_1.jpg
//   - data/file_2.jpg
//
// We also document this explicitly at
// https://aptrust.github.io/userguide/bagging/#aptrust-bagit-specification
// which says:
//
// "Tarred bags must untar to a single directory whose name matches the name of
// the tar file. For example, my_bag.tar must untar to a directory called my_bag."
//
// Once again, our code has relaxed the requirement saying the directory
// must match the bag name, but there still must BE a directory.
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

// StringIsShellSafe returns true if string looks safe to pass
// to shell.
func StringIsShellSafe(s string) bool {
	unsafeChars := "\"';{}|$` \t\r\n<>"
	return !strings.ContainsAny(s, unsafeChars)
}

// StripFileExtension returns filename, minus the extension.
// For example, "my_bag.tar" returns "my_bag".
func StripFileExtension(filename string) string {
	ext := filepath.Ext(filename)
	return filename[0 : len(filename)-len(ext)]
}

// PrintAndExit prints a message to STDERR and exits
func PrintAndExit(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}

// ProjectRoot returns the project root.
func ProjectRoot() string {
	_, thisFile, _, _ := runtime.Caller(0)
	absPath, _ := filepath.Abs(path.Join(thisFile, "..", ".."))
	return absPath
}

// TestsAreRunning returns true when code is running under "go test"
func TestsAreRunning() bool {
	return flag.Lookup("test.v") != nil
}

// RunningInCI returns true when code is running in the Travis CI
// environment.
func RunningInCI() bool {
	return os.Getenv("TRAVIS_BUILD_DIR") != ""
}

// Min returns the minimum of x or y without all the casting required
// by the math package.
func Min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

// EstimatedChunkSize returns the size we should use for each chunk
// in a multipart S3 upload. If we don't tell Minio what chunk size
// to use, and it doesn't know the size of the total upload, it
// tries to allocate 5 GB of RAM. This causes some restorations to
// fail with an out of memory error. https://trello.com/c/1hkP28x1
//
// Param totalSize is the total size of the object to upload.
// When restoring entire objects, we only know the approximate size,
// which will be IntellectualObject.FileSize plus one or more payload
// manifests and tag manifests of unknown size that we'll have to
// generate on the fly. In practice, we can guesstimate that the
// total size of a restored object will be about 1.01 - 1.1 times
// IntellectualObject.FileSize.
//
// Since S3 max upload size is 5 TB with 10k parts, the max this
// will return is 500MB for part size. Although we could return 5 GB,
// we don't want to because we can't allocate that much memory inside
// of memory-limited docker instances.
func EstimatedChunkSize(totalSize float64) uint64 {
	mb := float64(1024 * 1024)
	gb := float64(mb * 1024)
	minChunkSize := float64(5 * mb)
	maxChunkSize := float64(500 * mb)

	size := minChunkSize

	if totalSize >= float64(500*gb) {
		size = totalSize / 10000
	} else if totalSize >= float64(100*gb) {
		size = totalSize / 5000
	} else if totalSize >= float64(10*gb) {
		size = totalSize / 2500
	} else {
		size = totalSize / 500
	}

	// Size must be within bounds
	size = math.Min(size, maxChunkSize)
	size = math.Max(size, minChunkSize)

	return uint64(math.Ceil(size))
}

// ToHumanSize returns size in human-readable format.
func ToHumanSize(size int64) string {
	sizes := []string{"Bytes", "KB", "MB", "GB", "TB"}
	hs := float64(size)
	i := 0
	suffix := ""
	for i, suffix = range sizes {
		hs = float64(size) / math.Pow(1024, float64(i))
		if hs < 1024 {
			break
		}
	}
	return fmt.Sprintf("%.2f %s", hs, suffix)
}
