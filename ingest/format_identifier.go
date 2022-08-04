package ingest

import (
	ctx "context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/minio/minio-go/v7"
	"github.com/richardlehane/siegfried"
)

// CrashableFormats are extensions of file types the frequently cause
// Siegfried's MS-CFB reader to panic, bringing down the format identifier
// worker with it. We'd rather skip these than crash the worker, because
// requeueing will just crash the worker again.
var CrashableFormats = map[string]string{
	".accda":  "Microsoft Access",
	".accdb":  "Microsoft Access",
	".accde":  "Microsoft Access",
	".accdr":  "Microsoft Access",
	".accdt":  "Microsoft Access",
	".ade":    "Microsoft Access",
	".adn":    "Microsoft Access",
	".adp":    "Microsoft Access",
	".cdb":    "Microsoft Access",
	".doc":    "Microsoft Word",
	".docb":   "Microsoft Word",
	".docm":   "Microsoft Word",
	".docx":   "Microsoft Word",
	".dot":    "Microsoft Word",
	".dotx":   "Microsoft Word",
	".ecf":    "Outlook Add-In",
	".laccdb": "Microsoft Access",
	".ldb":    "Microsoft Access",
	".maf":    "Microsoft Access",
	".mam":    "Microsoft Access",
	".maq":    "Microsoft Access",
	".mar":    "Microsoft Access",
	".mat":    "Microsoft Access",
	".mda":    "Microsoft Access",
	".mdb":    "Microsoft Access",
	".mde":    "Microsoft Access",
	".mdf":    "Microsoft Access",
	".mdn":    "Microsoft Access",
	".mdw":    "Microsoft Access",
	".one":    "Microsoft One Note",
	".ost":    "Microsoft Outlook",
	".pa":     "Microsoft Powerpoint",
	".pot":    "Microsoft Powerpoint",
	".potm":   "Microsoft Powerpoint",
	".potx":   "Microsoft Powerpoint",
	".ppa":    "Microsoft Powerpoint",
	".ppam":   "Microsoft Powerpoint",
	".pps":    "Microsoft Powerpoint",
	".ppsm":   "Microsoft Powerpoint",
	".ppsx":   "Microsoft Powerpoint",
	".ppt":    "Microsoft Powerpoint",
	".pptm":   "Microsoft Powerpoint",
	".pptx":   "Microsoft Powerpoint",
	".pst":    "Microsoft Outlook",
	".sldm":   "Microsoft Powerpoint",
	".sldx":   "Microsoft Powerpoint",
	".wbk":    "Microsoft Word",
	".wll":    "Microsoft Word",
	".wwl":    "Microsoft Word",
	".xla_":   "Microsoft Excel",
	".xla":    "Microsoft Excel",
	".xla5":   "Microsoft Excel",
	".xla8":   "Microsoft Excel",
	".xlam":   "Microsoft Excel",
	".xll_":   "Microsoft Excel",
	".xll":    "Microsoft Excel",
	".xlm":    "Microsoft Excel",
	".xls":    "Microsoft Excel",
	".xlsb":   "Microsoft Excel",
	".xlsm":   "Microsoft Excel",
	".xlsx":   "Microsoft Excel",
	".xlt":    "Microsoft Excel",
	".xltm":   "Microsoft Excel",
	".xltx":   "Microsoft Excel",
	".xlw":    "Microsoft Excel",
	".xps":    "Windows XML document for printing",
}

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

		// Don't try to parse files that will crash Siegfried
		crashable, fileType := IsCrashableExtension(ingestFile.PathInBag)
		if crashable {
			fi.Context.Logger.Infof("Skipping crashable format (%s): %s", fileType, ingestFile.PathInBag)
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

		// TODO: Make sure s3Object is not nil?
		// Seems we shouldn't have to, but something is causing a nil
		// pointer exception inside of Siegfried. See Trello bug at
		// https://trello.com/c/ctfWzXZj
		//
		// Pass file name to Siegfried in lower-case because it can't do
		// extension-based identification on all-caps extensions lile .HTML, .PDF,
		// etc. We seem to get a lot of these all-caps files from old Windows systems.
		// https://trello.com/c/KDaWqqv0
		//
		// Looking at files in staging, this issue seems to affect only a small
		// handful of extensions. The most common is .JPG, which does not get
		// tagged as image/jpeg, while .jpg does.
		fi.Context.Logger.Infof("Starting ID of %s", ingestFile.Identifier())
		identifications, err := fi.Siegfried.Identify(s3Object, strings.ToLower(ingestFile.PathInBag), "")

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

// IsCrashableExtension returns true if file has an extension that may
// crash Siegfried.
func IsCrashableExtension(filename string) (bool, string) {
	// Don't try to parse files that will crash Siegfried
	ext := path.Ext(filename)
	fileType := CrashableFormats[ext]
	if fileType != "" {
		return true, fileType
	}
	if strings.HasPrefix(ext, ".smbdelete") {
		return true, "SMB Delete"
	}
	return false, ""
}
