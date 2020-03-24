package ingest

import (
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"net/url"
	"time"
)

// FormatIdentifier streams an S3 file, or the first chunk of it, through
// an external program to determine its file format. Currently, the tool
// is FIDO, which uses the PRONOM registry to identify formats.
type FormatIdentifier struct {
	IngestWorker
	FmtIdentifier *util.FormatIdentifier
}

// NewFormatIdentifier creates a new FormatIdentifier. This will panic
// if the prerequisites for running the format identifier script are
// not present.
func NewFormatIdentifier(context *common.Context, workItemId int, ingestObject *service.IngestObject) *FormatIdentifier {
	pathToScript := context.Config.FormatIdentifierScript()
	fmtIdentifier := util.NewFormatIdentifier(pathToScript)
	if !fmtIdentifier.CanRun() {
		panic(fmt.Sprintf("Missing prerequisites for format identifier. "+
			"Be sure the following are installed: curl, fido, python2, and "+
			"identify_format.sh. The last should be at %s", pathToScript))
	}
	return &FormatIdentifier{
		IngestWorker: IngestWorker{
			Context:      context,
			IngestObject: ingestObject,
			WorkItemId:   workItemId,
		},
		FmtIdentifier: fmtIdentifier,
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

func (fi *FormatIdentifier) IdentifyFormats() error {
	identify := func(ingestFile *service.IngestFile) error {
		// No need to re-identify if already id'd by FIDO
		if ingestFile.FormatIdentifiedBy == constants.FmtIdFido {
			return nil
		}
		key := fmt.Sprintf("%d/%s", fi.WorkItemId, ingestFile.UUID)
		signedURL, err := fi.GetPresignedURL(fi.Context.Config.StagingBucket, key)
		if err != nil {
			return err
		}
		idRecord, err := fi.FmtIdentifier.Identify(
			signedURL.String(),
			ingestFile.FidoSafeName())
		if err != nil {
			return err
		}

		// See comments above "if formatChanged" below.
		// formatChanged := (idRecord.MimeType != idRecord.MimeType)

		ingestFile.FileFormat = idRecord.MimeType
		ingestFile.FormatMatchType = idRecord.MatchType
		ingestFile.FormatIdentifiedBy = constants.FmtIdFido
		ingestFile.FormatIdentifiedAt = time.Now().UTC()

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

		return fi.IngestFileSave(ingestFile)
	}
	_, err := fi.Context.RedisClient.IngestFilesApply(fi.WorkItemId, identify)
	return err
}

// GetPresignedURL returns a pre-signed S3 URL that we can pass to the
// identify_format.sh script, so it can access the file without needing
// an S3 library.
func (fi *FormatIdentifier) GetPresignedURL(bucket, key string) (*url.URL, error) {
	urlParams := url.Values{}
	expires := time.Second * 24 * 60 * 60 * 7 // 7 days
	client := fi.Context.S3Clients[constants.S3ClientAWS]
	return client.PresignedGetObject(bucket, key, expires, urlParams)
}
