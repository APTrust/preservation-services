package ingest

import (
	"fmt"
	"net/url"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
)

// FormatIdentifier streams an S3 file, or the first chunk of it, through
// an external program to determine its file format. Currently, the tool
// is FIDO, which uses the PRONOM registry to identify formats.
type FormatIdentifier struct {
	Base
	FmtIdentifier *util.FormatIdentifier
}

// NewFormatIdentifier creates a new FormatIdentifier. This will panic
// if the prerequisites for running the format identifier script are
// not present.
func NewFormatIdentifier(context *common.Context, workItemID int, ingestObject *service.IngestObject) *FormatIdentifier {
	pathToScript := context.Config.FormatIdentifierScript()
	fmtIdentifier := util.NewFormatIdentifier(pathToScript)
	if !fmtIdentifier.CanRun() {
		panic(fmt.Sprintf("Missing prerequisites for format identifier. "+
			"Be sure the following are installed: curl, fido, python2, and "+
			"identify_format.sh. The last should be at %s", pathToScript))
	}
	return &FormatIdentifier{
		Base: Base{
			Context:      context,
			IngestObject: ingestObject,
			WorkItemID:   workItemID,
		},
		FmtIdentifier: fmtIdentifier,
	}
}

// Run runs all of the files belonging to this object's IngestObject
// the format identifier script. It saves format information data within each
// IngestFile record in Redis. If the script completes without error but is
// unable to identify a file, the IngestFile record will keep the original
// format identification supplied by constants.mime_types.go when the bag was
// scanned in an earlier phase of the ingest process.
//
// This returns the number of files that passed through the format identifier
// script without error, along with any error that did occur. Consider this
// successful if the returned count matches the IngestObject's FileCount.
// If not all files were identified, you re-run this function. It's intelligent
// enough to skip files that were successfully identified on a previous run.
func (fi *FormatIdentifier) Run() (int, []*service.ProcessingError) {
	identify := func(ingestFile *service.IngestFile) (errors []*service.ProcessingError) {
		// No need to re-identify if already id'd by FIDO
		if ingestFile.FormatIdentifiedBy == constants.FmtIdFido {
			return errors
		}
		key := fi.S3KeyFor(ingestFile)
		signedURL, err := fi.GetPresignedURL(fi.Context.Config.StagingBucket, key)
		if err != nil {
			errors = append(errors, fi.Error(ingestFile.Identifier(), err, false))
		}
		idRecord, err := fi.FmtIdentifier.Identify(
			signedURL.String(),
			ingestFile.FidoSafeName())
		if err != nil {
			errors = append(errors, fi.Error(ingestFile.Identifier(), err, false))
		}

		// See comments above "if formatChanged" below.
		// formatChanged := (idRecord.Succeeded && idRecord.MimeType != idRecord.MimeType)

		// The TarredBagScanner did an initial file format identification
		// when it scanned the bag, identifying by file extension. We want
		// to change the format only if FIDO actually succeeded in
		// identifying something. Otherwise, we stick with the original
		// id-by-extension.
		if idRecord.Succeeded {
			ingestFile.FileFormat = idRecord.MimeType
			ingestFile.FormatMatchType = idRecord.MatchType
			ingestFile.FormatIdentifiedBy = constants.FmtIdFido
			ingestFile.FormatIdentifiedAt = time.Now().UTC()
		}

		//
		// Here, we should update the object's Content-Type in S3,
		// but we can't. Minio supports updating user metadata using
		// CopyObject to copy an object over itself. If the user metadata
		// changes but the source and destination are the same, Minio
		// simply updates the user metadata. Unfortunately, the
		// ContentType is outside the user metadata and is not touched
		// in the copy process. We can and will still store the correct
		// mimetype in the GenericFile.FileFormat property in Pharos.
		//
		// if formatChanged {
		// 	fi.UpdateS3Metadata(ingestFile)
		// }

		return errors
	}

	// IngestFilesApply runs our function on all ingest file records
	// for the specified WorkItemId, and it saves each record back to
	// Redis.
	options := service.IngestFileApplyOptions{
		MaxErrors:   100,
		MaxRetries:  1,
		RetryMs:     0,
		SaveChanges: true,
		WorkItemID:  fi.WorkItemID,
	}
	return fi.Context.RedisClient.IngestFilesApply(identify, options)
}

// GetPresignedURL returns a pre-signed S3 URL that we can pass to the
// identify_format.sh script, so it can access the file without needing
// an S3 library.
func (fi *FormatIdentifier) GetPresignedURL(bucket, key string) (*url.URL, error) {
	urlParams := url.Values{}
	expires := time.Second * 24 * 60 * 60 * 7 // 7 days
	client := fi.Context.S3Clients[constants.StorageProviderAWS]
	return client.PresignedGetObject(bucket, key, expires, urlParams)
}
