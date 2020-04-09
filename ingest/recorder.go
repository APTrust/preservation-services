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

func (r *Recorder) RecordAll() (errors []*service.ProcessingError) {
	errors = r.recordObject()
	if len(errors) > 0 {
		return errors
	}
	errors = r.recordObjectEvents()
	if len(errors) > 0 {
		return errors
	}

	return errors
}

// recordObject records the IntellectualObject record in Pharos,
// along with the object-level events.
// The IntellectualObject comes from this worker's IngestObject.
// This method is public so we can test it. Call RecordAll() instead.
func (r *Recorder) recordObject() (errors []*service.ProcessingError) {
	obj := r.IngestObject.ToIntellectualObject()
	resp := r.Context.PharosClient.IntellectualObjectSave(obj)
	if resp.Error != nil {
		errors = append(errors, r.Error(r.IngestObject.Identifier(), resp.Error, true))
	} else {
		r.IngestObject.ID = resp.IntellectualObject().ID
	}
	err := r.IngestObjectSave()
	if err != nil {
		errors = append(errors, r.Error(r.IngestObject.Identifier(), err, false))
	}
	return errors
}

// recordObjectEvents records all of the object-level events for this
// ingest. File-level events are recorded separately in RecordFileEvents().
func (r *Recorder) recordObjectEvents() (errors []*service.ProcessingError) {
	for _, event := range r.IngestObject.GetIngestEvents() {
		resp := r.Context.PharosClient.PremisEventSave(event)
		if resp.Error != nil {
			errors = append(errors, r.Error(r.IngestObject.Identifier(), resp.Error, false))
		} else {
			event.ID = resp.PremisEvent().ID
		}
	}
	err := r.IngestObjectSave()
	if err != nil {
		errors = append(errors, r.Error(r.IngestObject.Identifier(), err, false))
	}
	return errors
}

// RecordFiles records all of this objects files, checksums, and premis
// events in Pharos. This method is public so we can test it.
// Call RecordAll() instead.
func (r *Recorder) RecordFiles() (errors []*service.ProcessingError) {

	return errors
}
