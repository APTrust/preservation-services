package ingest

import (
	"fmt"
	// "github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	// "github.com/minio/minio-go/v6"
	// "io"
	// "os/exec"
	"strings"
	// "time"
)

// FormatIdentifier streams an S3 file, or the first chunk of it, through
// an external program to determine its file format. Currently, the tool
// is FIDO, which uses the PRONOM registry to identify formats.
type FormatIdentifier struct {
	Context      *common.Context
	IngestObject *service.IngestObject
	WorkItemId   int
	CurlCmd      string
	PythonCmd    string
}

// NewFormatIdentifier creates a new FormatIdentifier.
func NewFormatIdentifier(context *common.Context, workItemId int, ingestObject *service.IngestObject) *FormatIdentifier {
	f := &FormatIdentifier{
		Context:      context,
		IngestObject: ingestObject,
		WorkItemId:   workItemId,
	}
	// Do this once to avoid unnecessary exec calls.
	curlCmd, pythonCmd := f.GetBaseCommands()
	f.CurlCmd = curlCmd
	f.PythonCmd = pythonCmd

	return f
}

// GetCommand returns a command for streaming S3 data through FIDO.
// This performs some rudimentary checks of the URL and filename to
// ensure we're not passing unsafe data to an external command. Since
// URLs come from pharos and follow a known format (s3host/bucket/uuid)
// and because we're constructing the filenames ourselves (uuid.ext),
// we should not have problems.
func (f *FormatIdentifier) GetCommandString(url, filename string) (string, error) {
	unsafeChars := "';{}|$"
	if strings.ContainsAny(url, unsafeChars) {
		return "", fmt.Errorf("URL contains unsafe characters")
	}
	if strings.ContainsAny(filename, unsafeChars) {
		return "", fmt.Errorf("File name contains unsafe characters")
	}
	cmdString := fmt.Sprintf("%s '%s' | %s -", f.CurlCmd, url, f.PythonCmd)
	if filename != "" {
		cmdString = fmt.Sprintf("%s '%s' | %s -filename='%s' -", f.CurlCmd, url, f.PythonCmd, filename)
	}
	return cmdString, nil
}

// GetBaseCommands returns the basic commands we'll need to run S3 data
// through FIDO. We should only call this one, when the FormatIdentifier
// object is constructed, so we don't wind up calling `which` three times
// for every file.
func (f *FormatIdentifier) GetBaseCommands() (string, string) {
	fido := f.PathTo("fido")
	python2 := f.PathTo("python2")
	curl := f.PathTo("curl")
	matchFormat := "OK,%(info.mimetype)s,%(info.matchtype)s\n"
	noMatchFormat := "FAIL,,%(info.matchtype)s\n"

	curlCmd := fmt.Sprintf("%s -s -r 0-524288 ", curl)
	pythonCmd := fmt.Sprintf(`%s -u %s -q -matchprintf="%s" -nomatchprintf="%s" -nocontainer`, python2, fido, matchFormat, noMatchFormat)

	return curlCmd, pythonCmd
}

// PathTo returns the path to the specified program, as returned by `which`.
// Because the FormatIdentifier cannot proceed if any of the required
// programs are missing (curl, python2, and fido), this call panics if the
// requested program cannot be found in the system's PATH.
func (f *FormatIdentifier) PathTo(program string) string {
	pathToProgram, err := util.PathTo(program)
	if err != nil || pathToProgram == "" {
		panic(fmt.Sprintf("Can't find %s in your PATH", program))
	}
	return pathToProgram
}
