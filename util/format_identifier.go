package util

import (
	"fmt"
	"os/exec"
	"strings"
)

type IdRecord struct {
	MatchType string
	MimeType  string
	Succeeded bool
}

type FormatIdentifier struct {
	hasPrerequisites bool
	pathToFido       string
}

// NewFormatIdentifier returns a new FormatIdentifier object.
func NewFormatIdentifier() *FormatIdentifier {
	pathToFido, _ := PathTo("fido")
	hasPrerequisites, _ := SystemHasIdentifierPrograms()
	return &FormatIdentifier{
		hasPrerequisites: hasPrerequisites,
		pathToFido:       pathToFido,
	}
}

// Identify returns an IdRecord containing FIDO's assessment of
// the file type and how it reached its conclusion. If FIDO was able
// to read the data but could not identify the file type, the value of
// IdRecord.Succeeded will be false. If FIDO was not able to run at all,
// the IdRecord will be nil, and this will return an error.
//
// While the FormatIdentifier object is reusable, you should call
// SystemHasIdentifierPrograms once before the first use to ensure that
// all external requirements are present.
func (f *FormatIdentifier) Identify(url, filename string) (*IdRecord, error) {
	if !f.hasPrerequisites {
		return nil, fmt.Errorf("System is missing one or more of: curl, fido, python2")
	}
	cmdString, err := f.GetCommandString(url, filename)
	if err != nil {
		return nil, err
	}
	output, err := exec.Command(cmdString).Output()
	if err != nil {
		return nil, err
	}
	idRecord := f.ParseOutput(strings.TrimSpace(string(output)))
	return idRecord, nil
}

// CanRun returns true if the system has all the prerequisites required
// to run FIDO.
func (f *FormatIdentifier) CanRun() bool {
	return f.hasPrerequisites
}

// PathToFido returns the full path to the system's FIDO installation.
func (f *FormatIdentifier) PathToFido() string {
	return f.pathToFido
}

// ParseOutput parses the output of the FIDO file identification command.
func (f *FormatIdentifier) ParseOutput(output string) *IdRecord {
	record := strings.Split(output, ",")
	idRecord := &IdRecord{
		MatchType: record[2],
		MimeType:  record[1],
	}
	if record[0] == "OK" {
		idRecord.Succeeded = true
	} else {
		idRecord.Succeeded = false
	}
	return idRecord
}

// GetCommand returns a command for streaming S3 data through FIDO.
// This performs some rudimentary checks of the URL and filename to
// ensure we're not passing unsafe data to an external command. Since
// URLs come from pharos and follow a known format (s3host/bucket/uuid)
// and because we're constructing the filenames ourselves (uuid.ext),
// we should not have problems.
func (f *FormatIdentifier) GetCommandString(url, filename string) (string, error) {
	err := f.ValidateParams(url, filename)
	if err != nil {
		return "", err
	}

	// Format strings for FIDO output.
	match := "OK,%(info.mimetype)s,%(info.matchtype)s"
	noMatch := "FAIL,,%(info.matchtype)s"

	// The full command pipes the output of curl to python2
	// in an unbuffered stream (-u) through STDIN. We fetch
	// about the first half megabyte, and -nocontainer tells
	// fido not to try to identify what's inside of zip files
	// and other containers. We just need to know the type of
	// the top-level file, and we don't want to fetch 50GB of
	// data to figure that out.
	cmdString := fmt.Sprintf(`curl -s -r 0-524288 '%s' | `+
		`python2 %s -q -matchprintf="%s" -nomatchprintf="%s" `+
		`-nocontainer -filename='%s' -`,
		url, f.pathToFido, match, noMatch, filename)
	return cmdString, nil
}

// ValidateParams returns an error if url or filename are empty, or
// if either appears not to be shell-safe.
func (f *FormatIdentifier) ValidateParams(url, filename string) error {
	if len(url) == 0 {
		return fmt.Errorf("URL cannot be empty")
	}
	if !StringIsShellSafe(url) {
		return fmt.Errorf("URL contains unsafe characters")
	}
	if len(filename) == 0 {
		return fmt.Errorf("File name cannot be empty")
	}
	if !StringIsShellSafe(filename) {
		return fmt.Errorf("File name contains unsafe characters")
	}
	return nil
}

// SystemHasIdentifierPrograms returns true if the system has the
// software we need to run file format identification. If this returns
// false, the string slice will contain a list of missing programs.
func SystemHasIdentifierPrograms() (bool, []string) {
	prerequisites := []string{
		"fido",
		"python2",
		"curl",
	}
	missing := make([]string, 0)
	for _, program := range prerequisites {
		pathToProgram, err := PathTo(program)
		if err != nil || pathToProgram == "" {
			missing = append(missing, program)
		}
	}
	return len(missing) == 0, missing
}
