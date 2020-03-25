package util

import (
	"fmt"
	"os/exec"
	"strings"
)

type IDRecord struct {
	MatchType string
	MimeType  string
	Succeeded bool
}

type FormatIdentifier struct {
	hasPrerequisites bool
	pathToScript     string
}

// NewFormatIdentifier returns a new FormatIdentifier object.
// Param pathToScript is the path to the format identifier script.
// You can get that from Config.PathTo("identify_format.sh")
func NewFormatIdentifier(pathToScript string) *FormatIdentifier {
	hasPrerequisites, _ := SystemHasIdentifierPrograms(pathToScript)
	return &FormatIdentifier{
		hasPrerequisites: hasPrerequisites,
		pathToScript:     pathToScript,
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
func (f *FormatIdentifier) Identify(url, filename string) (*IDRecord, error) {
	if !f.hasPrerequisites {
		return nil, fmt.Errorf("System is missing one or more of: curl, fido, python2, identify_format.sh")
	}
	err := f.ValidateParams(url, filename)
	if err != nil {
		return nil, err
	}
	output, err := exec.Command(f.pathToScript, url, filename).Output()
	if err != nil {
		errMsg := string(err.(*exec.ExitError).Stderr)
		return nil, fmt.Errorf("%s ... Command %s %s %s",
			strings.Replace(errMsg, "\n", "", -1),
			f.pathToScript, url, filename)
	}
	// fmt.Println(" >>>>>> ", filename, string(output))
	idRecord := f.ParseOutput(strings.TrimSpace(string(output)))
	return idRecord, nil
}

// CanRun returns true if the system has all the prerequisites required
// to run FIDO.
func (f *FormatIdentifier) CanRun() bool {
	return f.hasPrerequisites
}

// ParseOutput parses the output of the FIDO file identification command.
// The output may include more than one line. This parses the first line.
func (f *FormatIdentifier) ParseOutput(output string) *IDRecord {
	firstLine := strings.Split(output, "\n")[0]
	record := strings.Split(firstLine, ",")
	idRecord := &IDRecord{
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
//
// Param pathToScript is the path to the format identifier script.
// You can get that from Config.PathTo("identify_format.sh")
func SystemHasIdentifierPrograms(pathToScript string) (bool, []string) {
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
	if !FileExists(pathToScript) {
		missing = append(missing, pathToScript)
	}
	return len(missing) == 0, missing
}
