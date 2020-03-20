package ingest

import (
	"fmt"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
)

// FormatIdentifier streams an S3 file, or the first chunk of it, through
// an external program to determine its file format. Currently, the tool
// is FIDO, which uses the PRONOM registry to identify formats.
type FormatIdentifier struct {
	Context      *common.Context
	Identifier   *util.FormatIdentifier
	IngestObject *service.IngestObject
	WorkItemId   int
}

// NewFormatIdentifier creates a new FormatIdentifier. This will panic
// if the prerequisites for running the format identifier script are
// not present.
func NewFormatIdentifier(context *common.Context, workItemId int, ingestObject *service.IngestObject) *FormatIdentifier {
	pathToScript := context.Config.FormatIdentifierScript()
	identifier := util.NewFormatIdentifier(pathToScript)
	if !identifier.CanRun() {
		panic(fmt.Sprintf("Missing prerequisites for format identifier. "+
			"Be sure the following are installed: curl, fido, python2, and "+
			"identify_format.sh. The last should be at %s", pathToScript))
	}
	return &FormatIdentifier{
		Context:      context,
		Identifier:   identifier,
		IngestObject: ingestObject,
		WorkItemId:   workItemId,
	}
}

// TODO:
//
// For each IngestFile:
//
// Get Redis record
//   Skip if it already has a format identification timestamp and method
// Get signed URL for item in staging
// Run identifier on signed URL
//   If identification completes and fails, stick with the existing mime type
//   and note that ident tried and failed.
//   If identification cannot complete due to bad server response, proceed
//   to the next file.
//   Track whether all files complete & if any need retries.
// If format changed:
//   Save format back to Redis record
//   Use CopyObject to update ContentType metadata on S3 object
// Stamp Redis record with new ContentType, time of identification and method
//   (Method comes from identifier script: 'signature' or 'extension')
// Save Redis record
