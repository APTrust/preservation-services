package deletion

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	uuid "github.com/satori/go.uuid"
)

// Manager deletes files from preservation and ensures that Pharos
// IntellectualObjects, GenericFiles, StorageRecords and PremisEvents
// are updated to reflect the changes.
type Manager struct {
	// Context is the context, which includes config settings and
	// clients to access S3 and Pharos.
	Context *common.Context

	// Identifier is the identifier of the GenericFile or IntellectualObject
	// we're deleting.
	Identifier string

	// ItemType is the type of item we're deleting. It should be one of
	// constants.TypeFile or constants.TypeObject.
	ItemType string

	// WorkItemID is the ID of the WorkItem being processed.
	WorkItemID int

	// RequestedBy is the email address of the Pharos user who requested
	// (initiated) this deletion.
	RequestedBy string

	// InstApprover is the email address of the institututional admin who
	// approved this deletion.
	InstApprover string

	// APTrustApprover is the email address of the APTrust admin who
	// approved this deletion. This will be empty unless it was a bulk
	// deletion request. Normal deletion requests don't need APTrust approval.
	APTrustApprover string
}

// NewManager creates a new deletion.Manager.
func NewManager(context *common.Context, workItemID int, identifier, itemType, requestedBy, instApprover, aptrustApprover string) *Manager {
	return &Manager{
		Context:         context,
		Identifier:      identifier,
		ItemType:        itemType,
		WorkItemID:      workItemID,
		RequestedBy:     requestedBy,
		InstApprover:    instApprover,
		APTrustApprover: aptrustApprover,
	}
}

// Run deletes all copies of a single file from preservation/replication storage
// if Manager.ItemType is constants.TypeFile. If ItemType is constants.TypeObject,
// this deletes all copies of all of the object's files. This returns the number
// of GenericFiles deleted. The number of copies deleted my be higher. For example,
// deleting an object with 10 files from Standard storage deletes both the S3 and
// the Glacier copies. That's 20 stored object representing only 10 GenericFiles.
// This will return 10, not 20.
//
// It's up to the caller to ensure that the WorkItem has the proper approvals
// before calling this method.
func (m *Manager) Run() (count int, errors []*service.ProcessingError) {
	if m.RequestedBy == "" || m.InstApprover == "" {
		return 0, append(errors, m.Error(m.Identifier, fmt.Errorf("Deletion requires email of requestor and institutional approver"), true))
	}
	if m.ItemType == constants.TypeFile {
		count, errors = m.deleteSingleFile()
	} else {
		count, errors = m.deleteFiles()
	}
	return count, errors
}

// GetIngestObject is a dummy method that allows this object to conform to the
// ingest.Runnable interface.
func (m *Manager) GetIngestObject() *service.IngestObject {
	return nil
}

// deleteSingleFile is for deleting a single GenericFile. Call this when ItemType
// is GenericFile.
func (m *Manager) deleteSingleFile() (count int, errors []*service.ProcessingError) {
	resp := m.Context.PharosClient.GenericFileGet(m.Identifier)
	if resp.Error != nil {
		return count, append(errors, m.Error(m.Identifier, resp.Error, false))
	}
	gf := resp.GenericFile()
	if gf == nil {
		return count, append(errors, m.Error(m.Identifier, fmt.Errorf("Cannot find GenericFile with identifier %s", m.Identifier), false))
	}
	errs := m.deleteFile(gf)
	if len(errs) > 0 {
		return count, append(errors, errs...)
	}
	return 1, nil
}

// deleteFiles is for deleting all of the files belonging to an object.
// Call this when ItemType is IntellectualObject.
func (m *Manager) deleteFiles() (count int, errors []*service.ProcessingError) {
	params := url.Values{}
	params.Set("intellectual_object_identifier", m.Identifier)
	params.Set("page", "1")
	params.Set("per_page", "200")
	for {
		resp := m.Context.PharosClient.GenericFileList(params)
		if resp.Error != nil {
			errors = append(errors, m.Error(m.Identifier, resp.Error, false))
			return count, errors
		}
		for _, gf := range resp.GenericFiles() {
			if gf.State == constants.StateDeleted {
				continue
			}
			errs := m.deleteFile(gf)
			if len(errs) > 0 {
				errors = append(errors, errs...)
			} else {
				count++
			}
		}
		if resp.HasNextPage() {
			params = resp.ParamsForNextPage()
		} else {
			break
		}
	}
	if len(errors) == 0 {
		err := m.markObjectDeleted()
		if err != nil {
			errors = append(errors, m.Error(m.Identifier, err, false))
		}
	}
	return count, errors
}

// deleteFile tries to delete all the storage records associated with a file.
func (m *Manager) deleteFile(gf *registry.GenericFile) (errors []*service.ProcessingError) {
	resp := m.Context.PharosClient.StorageRecordList(gf.Identifier)
	if resp.Error != nil {
		return append(errors, m.Error(gf.Identifier, resp.Error, false))
	}
	// A single file can have multiple storage records.
	for _, sr := range resp.StorageRecords() {
		provider, bucket, key, err := m.Context.Config.ProviderBucketAndKeyFor(sr.URL)
		if err != nil {
			errors = append(errors, m.Error(gf.Identifier, err, false))
			continue
		}
		err = m.deleteFromPreservationStorage(provider, bucket, key)
		if err != nil {
			errors = append(errors, m.Error(gf.Identifier, err, false))
			continue
		}
		err = m.deleteStorageRecordFromPharos(gf, sr)
		if err != nil {
			errors = append(errors, m.Error(gf.Identifier, err, false))
			continue
		}
		err = m.saveFileDeletionEvent(gf, sr)
		if err != nil {
			errors = append(errors, m.Error(gf.Identifier, err, false))
		}
	}
	if len(errors) == 0 {
		resp = m.Context.PharosClient.GenericFileFinishDelete(gf.Identifier)
		if resp.Error != nil {
			errors = append(errors, m.Error(gf.Identifier, resp.Error, false))
		}
	}
	return errors
}

// deleteFromPreservationStroage deletes the copy of the file located
// in this S3/Glacier bucket. Note that a file may be saved in multiple
// buckets. This deletes from just one of those buckets.
func (m *Manager) deleteFromPreservationStorage(provider, bucket, key string) error {
	client := m.Context.S3Clients[provider]
	if client == nil {
		return fmt.Errorf("No S3 client for provider %s", provider)
	}
	err := client.RemoveObject(bucket, key)

	// We can ignore this message because the item may have been deleted
	// on a prior attempt.
	if err != nil && strings.Contains(err.Error(), "key does not exist") {
		m.Context.Logger.Warningf("Item %s %s/%s does not exist. May have been deleted in prior run.",
			provider, bucket, key)
		return nil
	}

	// Other errors are permission denied, bucket does not exist, conflict,
	// request limit. These need to be reported.
	return err
}

// deleteStorageRecordFromPharos deletes a single StorageRecord from
// Pharos. It does not touch the GenericFile record.
//
// TODO: Pharos also deletes these storage records when we mark
// the GenericFile deleted. However, it's probably better to do this
// here, in case we wind up deleting only one of two records. The
// PremisEvents will keep a record of what happened.
func (m *Manager) deleteStorageRecordFromPharos(gf *registry.GenericFile, sr *registry.StorageRecord) error {
	resp := m.Context.PharosClient.StorageRecordDelete(sr.ID)
	return resp.Error
}

// saveFileDeletionEvent saves a PremisEvent to Pharos saying we deleted
// one copy of this file from one preservation bucket. Other copies may
// exist. Note that we cannot call GenericFileFinishDelete until at least
// of these deletion events has been record in Pharos.
func (m *Manager) saveFileDeletionEvent(gf *registry.GenericFile, sr *registry.StorageRecord) error {
	eventId := uuid.NewV4()
	now := time.Now().UTC()
	outcomeDetail := m.RequestedBy
	outcomeInfo := fmt.Sprintf("File deleted at the request of %s.", m.RequestedBy)
	if m.InstApprover != "" {
		outcomeInfo += fmt.Sprintf(" Institutional approver: %s.", m.InstApprover)
	}
	if m.APTrustApprover != "" {
		outcomeInfo += fmt.Sprintf(" APTrust approver: %s.", m.APTrustApprover)
	}
	event := &registry.PremisEvent{
		Identifier:                   eventId.String(),
		EventType:                    constants.EventDeletion,
		DateTime:                     now,
		Detail:                       fmt.Sprintf("Deleted one copy of this file from %s", sr.URL),
		Outcome:                      constants.StatusSuccess,
		OutcomeDetail:                outcomeDetail,
		Object:                       "preservation-services + Minio S3 client",
		Agent:                        constants.S3ClientName,
		OutcomeInformation:           outcomeInfo,
		IntellectualObjectIdentifier: gf.IntellectualObjectIdentifier,
		GenericFileIdentifier:        gf.Identifier,
		InstitutionID:                gf.InstitutionID,
		IntellectualObjectID:         gf.IntellectualObjectID,
		CreatedAt:                    now,
		UpdatedAt:                    now,
	}

	resp := m.Context.PharosClient.PremisEventSave(event)
	return resp.Error
}

// markObjectDeleted tells Pharos that this object has been deleted in its
// entirety (all files deleted).
func (m *Manager) markObjectDeleted() error {
	resp := m.Context.PharosClient.IntellectualObjectFinishDelete(m.Identifier)
	return resp.Error
}

// Error returns a ProcessingError describing something that went wrong
// during processing.
func (m *Manager) Error(identifier string, err error, isFatal bool) *service.ProcessingError {
	return service.NewProcessingError(
		m.WorkItemID,
		identifier,
		err.Error(),
		isFatal,
	)
}
