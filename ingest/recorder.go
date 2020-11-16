package ingest

import (
	"fmt"
	"strings"

	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
)

// Recorder records the result of successful ingest in Pharos, creating
// or updating all necessary records, including IntellectualObject,
// GenericFiles, and PremisEvents.
type Recorder struct {
	Base
}

// NewRecorder returns a new Recorder.
func NewRecorder(context *common.Context, workItemID int, ingestObject *service.IngestObject) *Recorder {
	return &Recorder{
		Base: Base{
			Context:      context,
			IngestObject: ingestObject,
			WorkItemID:   workItemID,
		},
	}
}

// Run saves all object, file, checksum, and event data to Pharos.
// This returns the number of files saved, and a list of any errors that
// occurred.
func (r *Recorder) Run() (fileCount int, errors []*service.ProcessingError) {
	errors = r.recordObject()
	if len(errors) > 0 {
		return 0, errors
	}
	errors = r.recordObjectEvents()
	if len(errors) > 0 {
		return 0, errors
	}
	if r.IngestObject.RecheckFileIdentifiers {
		_, errors = r.recheckFileIdentifiers()
		if len(errors) > 0 {
			return 0, errors
		}
		r.IngestObject.RecheckFileIdentifiers = false
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
		err := r.IngestObjectSave()
		if err != nil {
			r.Context.Logger.Errorf("WorkItem %d. After marking ShouldDeletedFromReceiving = true, error saving IngestObject to Redis: %v", r.WorkItemID, err)
		}
	}
	if r.hasDuplicateIdentityError(errors) {
		r.IngestObject.RecheckFileIdentifiers = true
		err := r.IngestObjectSave()
		if err != nil {
			r.Context.Logger.Errorf("WorkItem %d. After marking RecheckFileIdentifiers = true, error saving IngestObject to Redis: %v", r.WorkItemID, err)
		}
	}
	return fileCount, errors
}

// recordObject records the IntellectualObject record in Pharos,
// along with the object-level events.
// The IntellectualObject comes from this worker's IngestObject.
// This method is public so we can test it. Call Run() instead.
func (r *Recorder) recordObject() (errors []*service.ProcessingError) {
	if r.IngestObject.SavedToRegistryAt.IsZero() {
		obj := r.IngestObject.ToIntellectualObject()
		resp := r.Context.PharosClient.IntellectualObjectSave(obj)
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

		resp := r.Context.PharosClient.PremisEventSave(event)
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

// recordFiles saves new files to Pharos in batches, and updates
// existing (reingested) files individually. Note that when we save
// GenericFile records, we save all associated PremisEvents, Checksums,
// and StorageRecords with the file. Typically, each batch of files we
// send to Pharos will have ~100 files and each of those files will have
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
		// Pharos can save new files in batches, but cannot
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
		genericFile, err := ingestFile.ToGenericFile()
		if err != nil {
			errors = append(errors, r.Error(ingestFile.Identifier(), err, false))
			return fileCount, errors
		}
		genericFiles[i] = genericFile
	}
	resp := r.Context.PharosClient.GenericFileSaveBatch(genericFiles)
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
// of requests to Pharos. One request per file.
func (r *Recorder) updateBatch(ingestFiles []*service.IngestFile) (fileCount int, errors []*service.ProcessingError) {
	for _, ingestFile := range ingestFiles {
		gf, err := ingestFile.ToGenericFile()
		if err != nil {
			errors = append(errors, r.Error(ingestFile.Identifier(), err, true))
		}
		resp := r.Context.PharosClient.GenericFileSave(gf)
		if resp.Error != nil {
			// TODO: Pharos should return 409 on StorageRecord.URL
			// conflict, and that should be a fatal error.
			errors = append(errors, r.Error(ingestFile.Identifier(), resp.Error, false))
			// -------- DEBUG --------
			jsonData, _ := gf.ToJSON()
			r.Context.Logger.Error(string(jsonData))
			// ------ END DEBUG ------
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
// Pharos.
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
		err := fmt.Errorf("Only %d of %d ingest files were marked as saved in Pharos", itemsMarked, len(ingestFiles))
		errors = append(errors, r.Error(r.IngestObject.Identifier(), err, false))
	}
	return errors
}

// hasDuplicateIdentityError returns true if we encountered an "identity has
// already been taken" error from Pharos. Ideally, we'd have a better way of
// testing for this, but this error occurs during batch operations, and Pharos
// does not report specifics about which error is a duplicate.
//
// This error occurs when a prior run of the ingest recorder successfully records
// a number of generic files but does not get a response from Pharos. There are
// several reasons for not getting a response, including proxy errors from Nginx,
// http timeouts, disk errors on the Pharos server, etc. Whatever the cause, we
// have to recover from it, so we set this flag. The next record worker to pick up
// this task will ask Pharos which generic files it knows about, and will set
// the proper ID on those files so we know to record them with a PUT/update
// instead of a POST/create.
func (r *Recorder) hasDuplicateIdentityError(errors []*service.ProcessingError) bool {
	for _, err := range errors {
		if strings.Contains(err.Message, `"identifier":["has already been taken"`) {
			return true
		}
	}
	return false
}

func (r *Recorder) recheckFileIdentifiers() (fileCount int, errors []*service.ProcessingError) {
	r.Context.Logger.Infof("WorkItem %d, object %s: Rechecking all GenericFile identifiers.", r.WorkItemID, r.IngestObject.Identifier())

	processFile := func(ingestFile *service.IngestFile) (errors []*service.ProcessingError) {
		resp := r.Context.PharosClient.GenericFileGet(ingestFile.Identifier())
		// We expect mostly 404s, because most files haven't been recorded.
		// We can return on 404, since there's nothing for us to do.
		if resp.ObjectNotFound() {
			return errors
		}
		if resp.Error != nil {
			return append(errors, r.Error(ingestFile.Identifier(), resp.Error, false))
		}

		// If this file is already in Pharos, copy its ID to our
		// IngestFile record. Note that the "SaveChanges: true"
		// setting in IngestFileApplyOptions means we will save the
		// updated IngestFile.ID back to Redis.
		pharosFile := resp.GenericFile()
		if pharosFile != nil {
			ingestFile.ID = pharosFile.ID
			r.Context.Logger.Infof("Set GenericFile.ID %d on %s.", ingestFile.ID, ingestFile.Identifier())
		}
		return errors
	}
	options := service.IngestFileApplyOptions{
		MaxErrors:   10,
		MaxRetries:  1,
		RetryMs:     0,
		SaveChanges: true,
		WorkItemID:  r.WorkItemID,
	}
	return r.Context.RedisClient.IngestFilesApply(processFile, options)
}
