package ingest

import (
	"fmt"

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

// // RecordFiles records all of this object's files, checksums, and file-level
// // Premis events in Pharos.
// func (r *Recorder) recordFiles() (fileCount int, errors []*service.ProcessingError) {
// 	// Be sure to save changes back to Redis, so that if recording fails
// 	// we can retry later without re-inserting already-saved files.
// 	// That will prevent "identifier already exists" errors from Premis events.
// 	options := service.IngestFileApplyOptions{
// 		MaxErrors:   3,
// 		MaxRetries:  1,
// 		RetryMs:     1000,
// 		SaveChanges: true,
// 		WorkItemID:  r.WorkItemID,
// 	}
// 	saveFn := r.getFileSaveFn(r.IngestObject)
// 	return r.Context.RedisClient.IngestFilesApply(saveFn, options)
// }

// // Save function saves a GenericFile to Pharos. The file is saved with its
// // checksums and premis events. The PharosClient figures out whether the save
// // should be a post/create or a put/update based on whether the GenericFile
// // has a non-zero ID.
// func (r *Recorder) getFileSaveFn(obj *service.IngestObject) service.IngestFileApplyFn {
// 	return func(ingestFile *service.IngestFile) (errors []*service.ProcessingError) {
// 		ingestFile.InstitutionID = obj.InstitutionID
// 		ingestFile.IntellectualObjectID = obj.ID
// 		ingestFile.ObjectIdentifier = obj.Identifier()
// 		// We save "preservable" files to Pharos, not bagit.txt, manifests, etc.
// 		if ingestFile.HasPreservableName() && ingestFile.SavedToRegistryAt.IsZero() {
// 			gf, err := ingestFile.ToGenericFile()
// 			if err != nil {
// 				errors = append(errors, r.Error(ingestFile.Identifier(), err, true))
// 			}
// 			resp := r.Context.PharosClient.GenericFileSave(gf)
// 			if resp.Error != nil {
// 				// TODO: Pharos should return 409 on StorageRecord.URL
// 				// conflict, and that should be a fatal error.
// 				errors = append(errors, r.Error(ingestFile.Identifier(), resp.Error, false))
// 				// -------- DEBUG --------
// 				jsonData, _ := gf.ToJSON()
// 				r.Context.Logger.Error(string(jsonData))
// 				// ------ END DEBUG ------
// 			} else {
// 				savedFile := resp.GenericFile()
// 				ingestFile.ID = savedFile.ID
// 				ingestFile.SavedToRegistryAt = savedFile.UpdatedAt
// 			}
// 		}
// 		return errors
// 	}
// }

func (r *Recorder) recordFiles() (fileCount int, errors []*service.ProcessingError) {
	return r.SaveFilesInBatches()
}

// SaveFilesInBatches saves new files to Pharos in batches, and updates
// existing (reingested) files individually.
func (r *Recorder) SaveFilesInBatches() (fileCount int, errors []*service.ProcessingError) {
	batchNumber := 0
	batchSize := int64(100)
	offset := uint64(0)
	for {
		batchNumber++
		fileMap, offset, err := r.Context.RedisClient.GetBatchOfFileKeys(
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
		filesToSave, filesToUpdate := r.PrepareFilesForSave(fileMap, batchNumber)
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
func (r *Recorder) PrepareFilesForSave(fileMap map[string]*service.IngestFile, batchNumber int) (filesToSave []*service.IngestFile, filesToUpdate []*service.IngestFile) {
	for _, ingestFile := range fileMap {
		ingestFile.InstitutionID = r.IngestObject.InstitutionID
		ingestFile.IntellectualObjectID = r.IngestObject.ID
		ingestFile.ObjectIdentifier = r.IngestObject.Identifier()
		if ingestFile.HasPreservableName() && ingestFile.SavedToRegistryAt.IsZero() {
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
	markAsSavedErrors := r.MarkFilesAsSaved(resp.GenericFiles(), ingestFiles)
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
func (r *Recorder) MarkFilesAsSaved(genericFiles []*registry.GenericFile, ingestFiles []*service.IngestFile) (errors []*service.ProcessingError) {
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
