package ingest

import (
	// "fmt"
	// "strings"
	// "time"

	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

// Recorder records the result of successful ingest in Pharos, creating
// or updating all necessary records, including IntellectualObject,
// GenericFiles, and PremisEvents.
type Recorder struct {
	Worker
}

// NewRecorder returns a new Recorder.
func NewRecorder(context *common.Context, workItemID int, ingestObject *service.IngestObject) *Recorder {
	return &Recorder{
		Worker: Worker{
			Context:      context,
			IngestObject: ingestObject,
			WorkItemID:   workItemID,
		},
	}
}

// RecordAll saves all object, file, checksum, and event data to Pharos.
// This returns the number of files saved, and a list of any errors that
// occurred.
func (r *Recorder) RecordAll() (fileCount int, errors []*service.ProcessingError) {
	errors = r.recordObject()
	if len(errors) > 0 {
		return 0, errors
	}
	errors = r.recordObjectEvents()
	if len(errors) > 0 {
		return 0, errors
	}
	fileCount, errors = r.recordFiles()
	return fileCount, errors
}

// recordObject records the IntellectualObject record in Pharos,
// along with the object-level events.
// The IntellectualObject comes from this worker's IngestObject.
// This method is public so we can test it. Call RecordAll() instead.
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

// RecordFiles records all of this object's files, checksums, and file-level
// Premis events in Pharos.
func (r *Recorder) recordFiles() (fileCount int, errors []*service.ProcessingError) {
	// Be sure to save changes back to Redis, so that if recording fails
	// we can retry later without re-inserting already-saved files.
	// That will prevent "identifier already exists" errors from Premis events.
	options := service.IngestFileApplyOptions{
		MaxErrors:   3,
		MaxRetries:  1,
		RetryMs:     1000,
		SaveChanges: true,
		WorkItemID:  r.WorkItemID,
	}
	saveFn := r.getFileSaveFn()
	return r.Context.RedisClient.IngestFilesApply(saveFn, options)
}

// Save function saves a GenericFile to Pharos. The file is saved with its
// checksums and premis events. The PharosClient figures out whether the save
// should be a post/create or a put/update based on whether the GenericFile
// has a non-zero ID.
func (r *Recorder) getFileSaveFn() service.IngestFileApplyFn {
	return func(ingestFile *service.IngestFile) (errors []*service.ProcessingError) {
		// We save "preservable" files to Pharos, not bagit.txt, manifests, etc.
		if ingestFile.HasPreservableName() && ingestFile.SavedToRegistryAt.IsZero() {
			gf, err := ingestFile.ToGenericFile()
			if err != nil {
				errors = append(errors, r.Error(ingestFile.Identifier(), err, true))
			}
			resp := r.Context.PharosClient.GenericFileSave(gf)
			if resp.Error != nil {
				errors = append(errors, r.Error(ingestFile.Identifier(), resp.Error, false))
			} else {
				savedFile := resp.GenericFile()
				ingestFile.ID = savedFile.ID
				ingestFile.SavedToRegistryAt = savedFile.UpdatedAt
			}
		}
		return errors
	}
}
