package common

import (
	"fmt"
	"os"
	"path"
	"runtime"
	"strings"
)

// These vars are set by the build script in scripts/build_partner_tools.rb
var (
	Version   string
	BuildDate string
	GitHash   string
	License   string
	WikiUrl   string
	RepoUrl   string
	Email     string
)

// Current Registry API Version
const RegistryAPIVersion = "v3"

// Exit Codes
const (
	// EXIT_OK means program completed successfully.
	EXIT_OK = 0
	// EXIT_RUNTIME_ERR means program did not complete
	// successfully due to an error. The error may have
	// occurred outside the program, such as a network
	// error or an error on a remote server.
	EXIT_RUNTIME_ERR = 1
	// EXIT_BAG_INVALID is used primarily for apt_validate.
	// It means the program completed its run and found that
	// the bag is not valid.
	EXIT_BAG_INVALID = 2
	// EXIT_USER_ERR means the user did not supply some
	// required option or argument, or the user supplied
	// invalid options/arguments.
	EXIT_USER_ERR = 3
	// EXIT_ITEM_NOT_FOUND can occur when the user tries to
	// upload, download, or validate a file that doesn't exist,
	// or when apt_check_ingest is asked to check on a bag
	// which has no record in Registry.
	EXIT_ITEM_NOT_FOUND = 4
	// EXIT_NOT_INGESTED occurs when apt_check_ingest finds
	// a WorkItem record for a bag in Registry, but the bag has
	// not yet been fully ingested.
	EXIT_NOT_INGESTED = 5
	// EXIT_SOME_INGESTED occurs when apt_check_ingest finds
	// multiple WorkItems in Registry matching a bag name, and
	// some versions of the bag have been ingested while others
	// have not.
	EXIT_SOME_INGESTED = 6
	// EXIT_NO_OP means the user requested help message or
	// version info. The program printed the info, and no other
	// operations were performed.
	EXIT_NO_OP = 100
)

func GetVersion() string {
	osName := strings.Title(runtime.GOOS)
	architecture := runtime.GOARCH
	appName := path.Base(os.Args[0])
	version := fmt.Sprintf("\n%s\n", appName)
	version += fmt.Sprintf("    Version %s for %s %s\n", Version, osName, architecture)
	version += fmt.Sprintf("    Commit %s. Built %s.\n", GitHash, BuildDate)
	version += fmt.Sprintf("    Released by APTrust.org under the %s license.\n", License)
	version += fmt.Sprintf("    Help: %s\n", Email)
	version += fmt.Sprintf("    More info at %s\n", WikiUrl)
	return version
}
