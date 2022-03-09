package ingest

import (
	ctx "context"
	"fmt"
	"path"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/minio/minio-go/v7"
	"github.com/richardlehane/siegfried"
)

// FormatIdentifier streams an S3 file, through Siegfried, which uses
// the PRONOM registry to identify formats.
type FormatIdentifier struct {
	Base
	Siegfried *siegfried.Siegfried
}

// NewFormatIdentifier creates a new FormatIdentifier. This will panic
// if the prerequisites for running the format identifier script are
// not present.
func NewFormatIdentifier(context *common.Context, workItemID int64, ingestObject *service.IngestObject) *FormatIdentifier {
	signatureFile := path.Join(context.Config.ProfilesDir, "default.sig")
	ziggy, err := siegfried.Load(signatureFile)
	if err != nil {
		panic(fmt.Sprintf("Siegfried cannot load signature file: %v", err))
	}
	return &FormatIdentifier{
		Base: Base{
			Context:      context,
			IngestObject: ingestObject,
			WorkItemID:   workItemID,
		},
		Siegfried: ziggy,
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
		// No need to re-identify if already id'd by Siegfried
		if ingestFile.FormatIdentifiedBy == constants.FmtIdSiegfried {
			return errors
		}
		// No point in identifying these. They're mostly .keep or __init__.py
		if ingestFile.Size == int64(0) {
			return errors
		}

		key := fi.S3KeyFor(ingestFile)

		s3Client := fi.Context.S3Clients[constants.StorageProviderAWS]
		s3Object, err := s3Client.GetObject(
			ctx.Background(),
			fi.Context.Config.StagingBucket,
			key,
			minio.GetObjectOptions{})

		errKey := fmt.Sprintf("%s (%s)", ingestFile.Identifier(), key)

		if err != nil {
			return append(errors, fi.Error(errKey, err, false))
		}

		defer s3Object.Close()

		identifications, err := fi.Siegfried.Identify(s3Object, ingestFile.PathInBag, "")

		if err != nil {
			// Siegfried can encounter a number of errors,
			// many of which are related to mscfb parsing.
			// (See https://github.com/richardlehane/mscfb,
			// A reader for Microsoft's Compound File Binary File Format.)
			// Other common errors warn (perhaps incorrectly)
			// of invalid zip file formats.
			// When these occur, we want to stick with the
			// original extension-based format identification
			// rather than letting the ingest process stall.
			// See https://trello.com/c/9ds5MYIt and
			// https://trello.com/c/9aIVRiM0.
			//
			// For now, our mission is to preserve what our
			// depositors send, not to reject materials because
			// our format identifier thinks the format is invalid.
			// We may want to take up this issue at a future member
			// meeting. Even if members wanted us to reject "invalid"
			// files, our definition of invalid may not match their
			// definition of invalid. So for now, log and preserve
			// everything.
			fi.Context.Logger.Warningf("Siegfried return error '%v' for file %s. Sticking with extension format %s", ingestFile.Identifier(), err, ingestFile.FileFormat)
		} else {
			mimeType := ""
			basis := ""
			for _, id := range identifications {
				mimeType, basis = GetMimeTypeFromLabels(fi.Siegfried.Label(id))
				if mimeType != "" {
					break
				}
			}
			// The TarredBagScanner did an initial file format identification
			// when it scanned the bag, identifying by file extension. We want
			// to change the format only if Siegfried actually succeeded in
			// identifying something. Otherwise, we stick with the original
			// id-by-extension.
			if mimeType != "" {
				ingestFile.FileFormat = mimeType
				ingestFile.FormatMatchType = basis
				ingestFile.FormatIdentifiedBy = constants.FmtIdSiegfried
				ingestFile.FormatIdentifiedAt = time.Now().UTC()
				fi.Context.Logger.Infof("Identified format of %s as %s", ingestFile.Identifier(), ingestFile.FileFormat)
			} else {
				fi.Context.Logger.Warningf("Could not identify format of %s. Leaving as %s", ingestFile.Identifier(), ingestFile.FileFormat)
			}

		}

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

// GetMimeTypeFromLabels returns the mime type and the basis for this match,
// based on label pairs extracted from Siegfried's identification record.
func GetMimeTypeFromLabels(labels [][2]string) (mimeType string, basis string) {
	for _, pair := range labels {
		if pair[0] == "mime" {
			mimeType = pair[1]
		} else if pair[0] == "basis" {
			basis = pair[1]
		}
	}
	return mimeType, basis
}
