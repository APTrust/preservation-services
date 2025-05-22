package ingest

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
)

// Recorder records the result of successful ingest in Registry, creating
// or updating all necessary records, including IntellectualObject,
// GenericFiles, and PremisEvents.
type Recorder struct {
	Base
}

// NewRecorder returns a new Recorder.
func NewRecorder(context *common.Context, workItemID int64, ingestObject *service.IngestObject) *Recorder {
	return &Recorder{
		Base: Base{
			Context:      context,
			IngestObject: ingestObject,
			WorkItemID:   workItemID,
		},
	}
}

// Run saves all object, file, checksum, and event data to Registry.
// This returns the number of files saved, and a list of any errors that
// occurred.
func (r *Recorder) Run() (fileCount int, errors []*service.ProcessingError) {
	if r.IngestObject.RecheckRegistryIdentifiers {
		errors = r.recheckRegistryIdentifiers()
		if len(errors) > 0 {
			r.flagPartialRecordingIfNecessary(errors)
			return 0, errors
		}
		r.IngestObjectSave()
	}
	errors = r.recordObject()
	if len(errors) > 0 {
		r.flagPartialRecordingIfNecessary(errors)
		return 0, errors
	}
	errors = r.recordObjectEvents()
	if len(errors) > 0 {
		r.flagPartialRecordingIfNecessary(errors)
		return 0, errors
	}
	fileCount, errors = r.recordFiles()
	if len(errors) == 0 {
		// This tells the cleanup process that it's safe to
		// delete the original tar file from the receiving bucket.
		// If the save fails and the cleanup worker doesn't get
		// the message, just log it. The tar file will eventually be
		// deleted by the bucket policy, but we want to know the
		// error occurred.
		r.IngestObject.ShouldDeleteFromReceiving = true
		r.IngestObject.RecheckRegistryIdentifiers = false
		err := r.IngestObjectSave()
		if err != nil {
			r.Context.Logger.Errorf("WorkItem %d. After marking ShouldDeletedFromReceiving = true, error saving IngestObject to Redis: %v", r.WorkItemID, err)
		}
	}
	r.flagPartialRecordingIfNecessary(errors)
	return fileCount, errors
}

// recordObject records the IntellectualObject record in Registry,
// along with the object-level events.
// The IntellectualObject comes from this worker's IngestObject.
// This method is public so we can test it. Call Run() instead.
func (r *Recorder) recordObject() (errors []*service.ProcessingError) {
	if r.IngestObject.SavedToRegistryAt.IsZero() {
		obj := r.IngestObject.ToIntellectualObject()
		resp := r.Context.RegistryClient.IntellectualObjectSave(obj)
		if resp.Error != nil {
			errors = append(errors, r.Error(r.IngestObject.Identifier(), resp.Error, true))
		} else {
			savedObject := resp.IntellectualObject()
			r.IngestObject.ID = savedObject.ID
			r.IngestObject.SavedToRegistryAt = savedObject.UpdatedAt
		}
		err := r.IngestObjectSave()
		if err != nil {
			errors = append(errors, r.Error(r.IngestObject.Identifier(), err, false))
		}
	}
	return errors
}

// recordObjectEvents records all of the object-level events for this
// ingest. File-level events are recorded separately in RecordFileEvents().
func (r *Recorder) recordObjectEvents() (errors []*service.ProcessingError) {
	for _, event := range r.IngestObject.GetIngestEvents() {
		// If event has non-zero ID, it's already been saved.
		// Attempting to re-save will cause an error.
		if event.ID > 0 {
			continue
		}

		// We have to set the object ID here, because we don't know
		// the ID until after we've inserted it in the Registry.
		// Also note that object-level events have a null GenericFileID,
		// since they pertain only to the object and not to any specific files.
		event.IntellectualObjectID = r.IngestObject.ID

		resp := r.Context.RegistryClient.PremisEventSave(event)
		if resp.Error != nil {
			errors = append(errors, r.Error(r.IngestObject.Identifier(), resp.Error, false))
		} else {
			savedEvent := resp.PremisEvent()
			event.ID = savedEvent.ID
			event.GenericFileID = savedEvent.GenericFileID
			event.InstitutionID = savedEvent.InstitutionID
			event.IntellectualObjectID = savedEvent.IntellectualObjectID
			event.CreatedAt = savedEvent.CreatedAt
			event.UpdatedAt = savedEvent.UpdatedAt
		}
	}
	err := r.IngestObjectSave()
	if err != nil {
		errors = append(errors, r.Error(r.IngestObject.Identifier(), err, false))
	}
	return errors
}

// recordFiles saves new files to Registry in batches, and updates
// existing (reingested) files individually. Note that when we save
// GenericFile records, we save all associated PremisEvents, Checksums,
// and StorageRecords with the file. Typically, each batch of files we
// send to Registry will have ~100 files and each of those files will have
// 2-3 checksums, 1-2 storage records, and 8-9 premis events.
func (r *Recorder) recordFiles() (fileCount int, errors []*service.ProcessingError) {
	batchNumber := 0
	batchSize := int64(100)
	offset := uint64(0)
	for {
		batchNumber++
		var fileMap map[string]*service.IngestFile
		var err error
		fileMap, offset, err = r.Context.RedisClient.GetBatchOfFileKeys(
			r.WorkItemID,
			offset,
			batchSize,
		)
		if err != nil {
			errors = append(errors, r.Error(r.IngestObject.Identifier(), err, false))
			break
		}
		// Registry can save new files in batches, but cannot
		// update existing files in batches, so we have to
		// separate these.
		filesToSave, filesToUpdate := r.prepareFilesForSave(fileMap, batchNumber)
		if len(filesToSave) > 0 {
			_, saveErrors := r.saveBatch(filesToSave)
			if len(saveErrors) > 0 {
				errors = append(errors, saveErrors...)
			}
		}
		if len(filesToUpdate) > 0 {
			_, updateErrors := r.updateBatch(filesToUpdate)
			if len(updateErrors) > 0 {
				errors = append(errors, updateErrors...)
			}
		}
		fileCount += len(fileMap)
		if offset == 0 {
			r.Context.Logger.Info("WorkItem %d: Reached end of file batches at %d", r.WorkItemID, fileCount)
			break
		}
	}
	return fileCount, errors
}

// PrepareFilesForSave sets the institution id, object id, and object identifier
// on each IngestFile object and divides files into a list to be saved and a
// list to be updated.
func (r *Recorder) prepareFilesForSave(fileMap map[string]*service.IngestFile, batchNumber int) (filesToSave []*service.IngestFile, filesToUpdate []*service.IngestFile) {
	for _, ingestFile := range fileMap {
		ingestFile.InstitutionID = r.IngestObject.InstitutionID
		ingestFile.IntellectualObjectID = r.IngestObject.ID
		ingestFile.ObjectIdentifier = r.IngestObject.Identifier()
		if ingestFile.HasPreservableName() && ingestFile.NeedsSave && ingestFile.SavedToRegistryAt.IsZero() {
			if ingestFile.ID == 0 {
				filesToSave = append(filesToSave, ingestFile)
			} else {
				filesToUpdate = append(filesToUpdate, ingestFile)
			}
		}
	}
	alreadySaved := len(fileMap) - (len(filesToSave) + len(filesToUpdate))
	r.Context.Logger.Infof("WorkItem %d: Batch %d has %d files. %d to save as new. %d to update. %d previously saved.", r.WorkItemID, batchNumber, len(fileMap), len(filesToSave), len(filesToUpdate), alreadySaved)
	return filesToSave, filesToUpdate
}

// saveBatch saves a batch of new GenericFiles in a single transaction.
func (r *Recorder) saveBatch(ingestFiles []*service.IngestFile) (fileCount int, errors []*service.ProcessingError) {
	genericFiles := make([]*registry.GenericFile, len(ingestFiles))
	for i, ingestFile := range ingestFiles {
		ingestFile.StorageOption = r.IngestObject.StorageOption
		genericFile, err := ingestFile.ToGenericFile()
		if err != nil {
			errors = append(errors, r.Error(ingestFile.Identifier(), err, false))
			return fileCount, errors
		}
		genericFiles[i] = genericFile
	}
	resp := r.Context.RegistryClient.GenericFileCreateBatch(genericFiles)
	if resp.Error != nil {
		errors = append(errors, r.Error(r.IngestObject.Identifier(), resp.Error, false))
		return fileCount, errors
	}
	markAsSavedErrors := r.markFilesAsSaved(resp.GenericFiles(), ingestFiles)
	if len(markAsSavedErrors) > 0 {
		errors = append(errors, markAsSavedErrors...)
	}
	return len(ingestFiles), errors
}

// updateBatch updates a batch of existing GenericFile records in a series
// of requests to Registry. One request per file.
func (r *Recorder) updateBatch(ingestFiles []*service.IngestFile) (fileCount int, errors []*service.ProcessingError) {
	for _, ingestFile := range ingestFiles {

		// A.D. May 22, 2025
		//
		// The if statement below pertains to https://trello.com/c/C4XlgSNU
		// and https://trello.com/c/ccxvAQkv, which was a re-ingest bug that
		// caused some files to be stored with two different storage options,
		// leading to us having two different, out-of-sync versions of a file
		// in preservation storage.
		//
		// To trigger this bug, a depositor had to ingest a bag with storage
		// option X and then re-ingest it with storage option Y. In that case,
		// our system should have forced all of the files into option X.
		//
		// In practice, however, it left the old files in X and saved new
		// versions to Y.
		//
		// This is a rare bug, affecting only 5 of the 40 million files
		// we ingested between 2014 and 2025.
		//
		// Changes to metadata_gatherer.go and reingest_manager.go
		// should prevent this from ever happening again. Still, we
		// include one last check here to see if it does somehow happen.
		//
		// If it does, we want an admin to know. The admin will be able
		// to fix the problem manually, since we will have all of the
		// object's files. The admin's job will be to make sure they are
		// all in the right place.
		//
		// Again, this should be impossible after the changes of May 22, 2025.
		//
		if ingestFile.StorageOption != r.IngestObject.StorageOption {
			errMismatch := fmt.Sprintf(
				"Storage option %s for file %s does not match object storage option %s. This shouldn't happen and will lead to mismatched file versions in different storage locations. See https://trello.com/c/C4XlgSNU and https://trello.com/c/ccxvAQkv.",
				ingestFile.StorageOption,
				ingestFile.Identifier(),
				r.IngestObject.StorageOption,
			)
			r.Context.Logger.Error(errMismatch)
			// Don't mark the error as fatal, because we want the
			// recorder to finish recording all of the info for
			// this ingest.
			errors = append(errors, service.NewProcessingError(r.WorkItemID, ingestFile.Identifier(), errMismatch, false))
		}
		gf, err := ingestFile.ToGenericFile()
		if err != nil {
			errors = append(errors, r.Error(ingestFile.Identifier(), err, true))
		}
		resp := r.Context.RegistryClient.GenericFileSave(gf)
		if resp.Error != nil {
			// TODO: Pharos should return 409 on StorageRecord.URL
			// conflict, and that should be a fatal error.
			// Is this fixed in Registry?
			errors = append(errors, r.Error(ingestFile.Identifier(), resp.Error, false))
		} else {
			savedFile := resp.GenericFile()
			ingestFile.ID = savedFile.ID
			ingestFile.SavedToRegistryAt = savedFile.UpdatedAt
			err := r.IngestFileSave(ingestFile)
			if err != nil {
				errors = append(errors, r.Error(ingestFile.Identifier(), err, false))
			}
		}
	}
	return len(ingestFiles), errors
}

// MarkFilesAsSaved updates files in Redis to indicate they were saved to
// Registry.
func (r *Recorder) markFilesAsSaved(genericFiles []*registry.GenericFile, ingestFiles []*service.IngestFile) (errors []*service.ProcessingError) {
	itemsMarked := 0
	ingestFileMap := make(map[string]*service.IngestFile, len(ingestFiles))
	for _, ingestFile := range ingestFiles {
		ingestFileMap[ingestFile.Identifier()] = ingestFile
	}
	for _, genericFile := range genericFiles {
		ingestFile := ingestFileMap[genericFile.Identifier]
		ingestFile.ID = genericFile.ID
		ingestFile.SavedToRegistryAt = genericFile.UpdatedAt
		err := r.IngestFileSave(ingestFile)
		if err != nil {
			errors = append(errors, r.Error(ingestFile.Identifier(), err, false))
		}
		itemsMarked++
	}
	if itemsMarked < len(ingestFiles) {
		err := fmt.Errorf("Only %d of %d ingest files were marked as saved in Registry", itemsMarked, len(ingestFiles))
		errors = append(errors, r.Error(r.IngestObject.Identifier(), err, false))
	}
	return errors
}

// hasDuplicateIdentityError returns true if we encountered an "identity has
// already been taken" error from Registry. Ideally, we'd have a better way of
// testing for this, but this error occurs during batch operations, and Registry
// does not report specifics about which error is a duplicate.
//
// This error occurs when a prior run of the ingest recorder successfully records
// a number of generic files but does not get a response from Registry. There are
// several reasons for not getting a response, including proxy errors from Nginx,
// http timeouts, disk errors on the Registry server, etc. Whatever the cause, we
// have to recover from it, so we set this flag. The next record worker to pick up
// this task will ask Registry which generic files it knows about, and will set
// the proper ID on those files so we know to record them with a PUT/update
// instead of a POST/create.
//
// Pharos really should be returning 409 here, not 422.
// Is this fixed in Registry?
//
// https://trello.com/c/edO9DaqO/700-handle-422-identifier-already-in-use
func (r *Recorder) hasDuplicateIdentityError(errors []*service.ProcessingError) bool {
	for _, err := range errors {
		if strings.Contains(err.Message, "has already been taken") {
			return true
		}
	}
	return false
}

// https://trello.com/c/edO9DaqO/700-handle-422-identifier-already-in-use
func (r *Recorder) recheckRegistryIdentifiers() []*service.ProcessingError {
	objectExistsInRegistry, errors := r.recheckRegistryObject()

	// Can't continue on error; don't need to if object doesn't exist
	if len(errors) > 0 || objectExistsInRegistry == false {
		return errors
	}

	return r.recheckRegistryFiles()
}

// https://trello.com/c/edO9DaqO/700-handle-422-identifier-already-in-use
func (r *Recorder) recheckRegistryObject() (objectExistsInRegistry bool, errors []*service.ProcessingError) {
	// If we already have the object id, no need to bother Registry,
	// except in the edge case where Registry has the object and
	// only SOME of the object events.
	if r.IngestObject.ID > 0 {
		r.recheckObjectEvents()
		err := r.IngestObjectSave()
		if err != nil {
			errors = append(errors, r.Error(r.IngestObject.Identifier(), err, false))
		}
		return true, errors
	}

	r.Context.Logger.Infof("Checking for existing Registry object %s", r.IngestObject.Identifier())

	// Look up the object in Registry
	resp := r.Context.RegistryClient.IntellectualObjectByIdentifier(r.IngestObject.Identifier())
	if resp.Error != nil {
		// If not found, item has not yet been recorded, and we have
		// no work to do here.
		if resp.Response.StatusCode == http.StatusNotFound {
			return false, errors
		} else {
			errors = append(errors, r.Error(r.IngestObject.Identifier(), resp.Error, false))
			return false, errors
		}
	}
	obj := resp.IntellectualObject()
	if obj == nil {
		errors = append(errors, r.Error(r.IngestObject.Identifier(), fmt.Errorf("Registry returned nil object"), false))
		return false, errors
	}
	r.Context.Logger.Infof("RecheckRegistryObject: Setting object ID to %d for %s", obj.ID, obj.Identifier)
	r.IngestObject.ID = obj.ID
	r.recheckObjectEvents()
	err := r.IngestObjectSave()
	if err != nil {
		errors = append(errors, r.Error(r.IngestObject.Identifier(), err, false))
	}
	return true, errors
}

// https://trello.com/c/edO9DaqO/700-handle-422-identifier-already-in-use
// If Registry has already recorded the object-level ingest events, we need
// to know their IDs so we don't try to re-record them.
func (r *Recorder) recheckObjectEvents() {
	for _, event := range r.IngestObject.PremisEvents {
		if event.ID > 0 {
			continue
		}
		resp := r.Context.RegistryClient.PremisEventByIdentifier(event.Identifier)
		if resp.Error == nil && resp.PremisEvent() != nil {
			event.ID = resp.PremisEvent().ID
		}
	}
}

// https://trello.com/c/edO9DaqO/700-handle-422-identifier-already-in-use
// Check GenericFile records in Registry. If they have IDs, we need to copy them
// to our Redis records before recording. This is part of the recovery process
// for partial ingest recording.
func (r *Recorder) recheckRegistryFiles() (errors []*service.ProcessingError) {
	params := url.Values{}
	params.Set("intellectual_object_identifier", r.IngestObject.Identifier())
	params.Set("page", "1")
	params.Set("per_page", "200")
	for {
		resp := r.Context.RegistryClient.GenericFileList(params)
		if resp.Error != nil {
			errors = append(errors, r.Error(r.IngestObject.Identifier(), resp.Error, false))
			break // go to return errors
		}
		for _, gf := range resp.GenericFiles() {
			r.updateRedisFileAndEvents(gf)
		}
		if resp.HasNextPage() {
			params = resp.ParamsForNextPage()
		} else {
			break
		}
	}
	return errors
}

// https://trello.com/c/edO9DaqO/700-handle-422-identifier-already-in-use
func (r *Recorder) updateRedisFileAndEvents(gf *registry.GenericFile) (errors []*service.ProcessingError) {
	// Update Redis IngestFile record
	ingestFile, _ := r.Context.RedisClient.IngestFileGet(r.WorkItemID, gf.Identifier)
	// Registry may have some older files for this object that are not
	// part of this ingest. Redis will return nil for those.
	if ingestFile != nil {
		ingestFile.ID = gf.ID
		for _, event := range gf.PremisEvents {
			eventToRecord := ingestFile.FindEvent(event.Identifier)
			if eventToRecord != nil {
				eventToRecord.ID = event.ID
			}
		}
		err := r.Context.RedisClient.IngestFileSave(r.WorkItemID, ingestFile)
		if err != nil {
			errors = append(errors, r.Error(r.IngestObject.Identifier(), err, false))
			return errors
		}
	}
	return errors
}

func (r *Recorder) flagPartialRecordingIfNecessary(errors []*service.ProcessingError) {
	if r.hasDuplicateIdentityError(errors) {
		r.IngestObject.RecheckRegistryIdentifiers = true
		err := r.IngestObjectSave()
		if err != nil {
			r.Context.Logger.Errorf("WorkItem %d. After marking RecheckRegistryIdentifiers = true, error saving IngestObject to Redis: %v", r.WorkItemID, err)
		} else {
			r.Context.Logger.Errorf("Flagged WorkItem %d, object %s as partially recorded and in need of duplicate identifier check ", r.WorkItemID, r.IngestObject.Identifier())
		}
	}

}
