package restoration

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/network/glacier"
)

// Glacier response statuses. Pending means the restoration has been
// requested but not completed. Completed means the item has been copied
// back to S3. Error means something went wrong.
const (
	RestorePending = iota
	RestoreCompleted
	RestoreError
)

// GlacierRestorer initiates restoration of an object from Glacier to S3.
// From there, the BagRestorer or FileRestorer can move items to the
// depositor's restoration bucket.
type GlacierRestorer struct {
	Base
}

// NewGlacierRestorer creates a new GlacierRestorer
func NewGlacierRestorer(context *common.Context, workItemID int, restorationObject *service.RestorationObject) *GlacierRestorer {
	return &GlacierRestorer{
		Base: Base{
			Context:           context,
			RestorationObject: restorationObject,
			WorkItemID:        workItemID,
		},
	}
}

// Run initiates or checks on the Glacier restore requests for a single file or for
// all of the files that make up an intellectual object. The first call to run
// initiates restoration requests. Subsequent calls check on the progress of the
// restoration.
//
// This will return a non-fatal error unless all of the requested restorations
// are available in S3.
func (r *GlacierRestorer) Run() (fileCount int, errors []*service.ProcessingError) {
	if r.RestorationObject.RestorationType == constants.RestorationTypeFile {
		status, errors := r.restoreFile()
		if len(errors) == 0 && status == RestoreCompleted {
			r.RestorationObject.AllFilesRestored = true
			r.RestorationObject.RestoredAt = time.Now().UTC()
		}
		fileCount = 1
	} else {
		completed, pending, errored, errors := r.restoreAllFiles()
		if completed > 1 && pending == 0 && errored == 0 && len(errors) == 0 {
			r.RestorationObject.AllFilesRestored = true
			r.RestorationObject.RestoredAt = time.Now().UTC()
		}
		fileCount = completed + pending + errored
	}

	// If we have no errors but not all files are ready in S3,
	// return a non-fatal error so the worker will requeue this
	// item and check it again in a few hours.
	if len(errors) == 0 && !r.RestorationObject.AllFilesRestored {
		err := fmt.Errorf("Initiated restore, but files are not yet available in S3. Requeued for later recheck.")
		errors = append(errors, r.Error(r.RestorationObject.Identifier, err, false))
	}

	return fileCount, errors
}

// Restore all of the files belonging to an IntellectualObject.
func (r *GlacierRestorer) restoreAllFiles() (completed, pending, errored int, errors []*service.ProcessingError) {
	pageNumber := 1
	for {
		files, err := GetBatchOfFiles(r.Context, r.RestorationObject.Identifier, pageNumber)
		if err != nil {
			errors = append(errors, r.Error(r.RestorationObject.Identifier, err, false))
			return completed, pending, errored, errors
		}
		for _, gf := range files {
			restoreStatus, errs := r.requestRestoration(gf)
			errors = errs
			switch restoreStatus {
			case RestoreCompleted:
				completed++
			case RestorePending:
				pending++
			case RestoreError:
				errored++
			}
		}
		if len(files) == 0 {
			break
		}
		pageNumber++
	}
	return completed, pending, errored, errors
}

// Restore a single file from Glacier to S3
func (r *GlacierRestorer) restoreFile() (restoreStatus int, errors []*service.ProcessingError) {
	resp := r.Context.PharosClient.GenericFileGet(r.RestorationObject.Identifier)
	if resp.Error != nil {
		errors = append(errors, r.Error(r.RestorationObject.Identifier, resp.Error, true))
		return RestoreError, errors
	}
	gf := resp.GenericFile()
	if gf == nil {
		err := fmt.Errorf("Pharos returned nil for GenericFile %s", r.RestorationObject.Identifier)
		errors = append(errors, r.Error(r.RestorationObject.Identifier, err, true))
		return RestoreError, errors
	}
	return r.requestRestoration(gf)
}

// Send the restoration request to Glacier and return the status.
func (r *GlacierRestorer) requestRestoration(gf *registry.GenericFile) (restoreStatus int, errors []*service.ProcessingError) {
	_, storageRecord, err := BestRestorationSource(r.Context, gf)
	if err != nil {
		errors = append(errors, r.Error(gf.Identifier, err, true))
		return RestoreError, errors
	}
	statusCode, body, err := glacier.Restore(r.Context, storageRecord.URL)
	if err != nil {
		errors = append(errors, r.Error(gf.Identifier, err, false))
		return RestoreError, errors
	}
	if statusCode == http.StatusNotFound {
		err = fmt.Errorf("Glacier returned 404 - object not found.")
		errors = append(errors, r.Error(gf.Identifier, err, true))
		return RestoreError, errors
	} else {
		r.Context.Logger.Infof("Glacier returned StatusCode %d for %s", statusCode, gf.Identifier)
	}

	switch statusCode {
	case http.StatusOK:
		// 200 means item has already been restored to S3
		r.Context.Logger.Infof("File %s (%s) has been copied to S3", gf.Identifier, gf.UUID())
		restoreStatus = RestoreCompleted
	case http.StatusForbidden:
		// 403 means item is in S3 storage class, not Glacier.
		// We'll hit this often when testing on staging and demo,
		// where items take a day or so to transition from S3
		// to Glacier. As long as it's in S3, we're OK to proceed.
		if r.isInS3StorageClass(body) {
			r.Context.Logger.Infof("File %s (%s) is already in S3 storage class", gf.Identifier, gf.UUID())
			restoreStatus = RestoreCompleted
		} else {
			// If this is forbidden for some other reason, it's likely
			// bad credentials or a misconfigured bucket. Neither case
			// should ever happen, and both require admin intervention.
			r.Context.Logger.Errorf("File %s (%s): forbidden, but not because of storage class.", gf.Identifier, gf.UUID())
			err = fmt.Errorf("Glacier returned status %d: %s", statusCode, body)
			errors = append(errors, r.Error(gf.Identifier, err, true))
			restoreStatus = RestoreError
		}
	case http.StatusAccepted:
		// 202/Accepted means the restore request has been queued
		r.Context.Logger.Infof("Restoration request for %s (%s) has been accepted", gf.Identifier, gf.UUID())
		restoreStatus = RestorePending
	case http.StatusConflict:
		// 409/Conflict means restore request is in progress
		r.Context.Logger.Infof("Restoration request for %s (%s) was accepted earlier and is pending", gf.Identifier, gf.UUID())
		restoreStatus = RestorePending
	case http.StatusServiceUnavailable:
		r.Context.Logger.Infof("Restoration request for %s (%s) temporarily denied: expedited restore service unavailable. Try again later.", gf.Identifier, gf.UUID())
		restoreStatus = RestorePending
	default:
		err = fmt.Errorf("Glacier returned unexpected status %d: %s", statusCode, body)
		errors = append(errors, r.Error(gf.Identifier, err, true))
		restoreStatus = RestoreError
	}
	return restoreStatus, errors
}

func (r *GlacierRestorer) isInS3StorageClass(body string) bool {
	// Body is typically ~200 bytes of XML
	return strings.Contains(body, "InvalidObjectState")
}

/*
<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>InvalidObjectState</Code><Message>Restore is not allowed for the object's current storage class</Message><RequestId>0R4VDR7M1W9T6GAW</RequestId><HostId>AxVAZQwthloiJJwhIvwctT27qD1uCAu/AihSPwLXQ63hXi5QGH1GjMRhl6Y+iPgNUrY2hkm0EYw=</HostId></Error>
*/
