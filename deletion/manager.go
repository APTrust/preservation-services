package deletion

import (
	ctx "context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/network"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
)

// Manager deletes files from preservation and ensures that Registry
// IntellectualObjects, GenericFiles, StorageRecords and PremisEvents
// are updated to reflect the changes.
type Manager struct {
	// Context is the context, which includes config settings and
	// clients to access S3 and Registry.
	Context *common.Context

	// Identifier is the identifier of the GenericFile or IntellectualObject
	// we're deleting.
	Identifier string

	// ItemType is the type of item we're deleting. It should be one of
	// constants.TypeFile or constants.TypeObject.
	ItemType string

	// WorkItemID is the ID of the WorkItem being processed.
	WorkItemID int64

	// RequestedBy is the email address of the Registry user who requested
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
func NewManager(context *common.Context, workItemID int64, identifier, itemType, requestedBy, instApprover, aptrustApprover string) *Manager {
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
//
// After deleting files from storage, this method creates deletion PREMIS events
// in Registry for each file, and it changes the state of each file from "A" (active)
// to "D" (deleted). For object deletion, it also changes the Registry object's
// state to "D" if all file deletions succeeded.
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

// IngestObjectGet is a dummy method that allows this object to conform to the
// ingest.Runnable interface.
func (m *Manager) IngestObjectGet() *service.IngestObject {
	return nil
}

// IngestObjectSave is a dummy method that allows this object to conform to the
// ingest.Runnable interface.
func (m *Manager) IngestObjectSave() error {
	return nil
}

// deleteSingleFile is for deleting a single GenericFile. Call this when ItemType
// is GenericFile.
func (m *Manager) deleteSingleFile() (count int, errors []*service.ProcessingError) {
	resp := m.Context.RegistryClient.GenericFileByIdentifier(m.Identifier)
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
		resp := m.Context.RegistryClient.GenericFileList(params)
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
	params := url.Values{}
	params.Add("generic_file_identifier", gf.Identifier)
	resp := m.Context.RegistryClient.StorageRecordList(params)
	if resp.Error != nil {
		return append(errors, m.Error(gf.Identifier, resp.Error, false))
	}
	// A single file can have multiple storage records.
	for _, sr := range resp.StorageRecords() {
		bucket, key, err := m.Context.Config.BucketAndKeyFor(sr.URL)
		if err != nil {
			errors = append(errors, m.Error(gf.Identifier, err, false))
			continue
		}
		err = m.deleteFromPreservationStorage(bucket, key)
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
		resp = m.Context.RegistryClient.GenericFileDelete(gf.Identifier)
		if resp.Error != nil {
			errors = append(errors, m.Error(gf.Identifier, resp.Error, false))
		}
	}
	return errors
}

// deleteFromPreservationStroage deletes the copy of the file located
// in this S3/Glacier bucket. Note that a file may be saved in multiple
// buckets. This deletes from just one of those buckets.
func (m *Manager) deleteFromPreservationStorage(bucket *common.PreservationBucket, key string) error {
	client := m.Context.S3Clients[bucket.Provider]
	if client == nil {
		return fmt.Errorf("No S3 client for provider %s", bucket.Provider)
	}
	err := client.RemoveObject(ctx.Background(), bucket.Bucket, key, minio.RemoveObjectOptions{})

	// We can ignore this message because the item may have been deleted
	// on a prior attempt.
	if err != nil {
		if strings.Contains(err.Error(), "key does not exist") {
			m.Context.Logger.Warningf("Item %s %s/%s does not exist. May have been deleted in prior run.", bucket.Provider, bucket.Bucket, key)
			return nil
		}
		if err.Error() == "Access Denied" && strings.Contains(bucket.Host, "wasabi") {
			err = fmt.Errorf("%v - Note that Wasabi has a minimum storage period of 30 days. Deletions before then will be denied.", err)
		}

		m.Context.Logger.Errorf("Attempt to delete item %s %s/%s failed. Provider returned: %v", bucket.Provider, bucket.Bucket, key, err)
	} else {
		m.Context.Logger.Infof("Delete item %s %s/%s", bucket.Provider, bucket.Bucket, key)
	}

	// Other errors are permission denied, bucket does not exist, conflict,
	// request limit. These need to be reported.
	return err
}

// saveFileDeletionEvent saves a PremisEvent to Registry saying we deleted
// one copy of this file from one preservation bucket. Other copies may
// exist. Note that we cannot call GenericFileFinishDelete until at least
// of these deletion events has been record in Registry.
func (m *Manager) saveFileDeletionEvent(gf *registry.GenericFile, sr *registry.StorageRecord) error {
	eventId := uuid.New()
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
		Identifier:           eventId.String(),
		EventType:            constants.EventDeletion,
		DateTime:             now,
		Detail:               fmt.Sprintf("Deleted one copy of this file from %s", sr.URL),
		Outcome:              constants.StatusSuccess,
		OutcomeDetail:        outcomeDetail,
		Object:               "preservation-services + Minio S3 client",
		Agent:                constants.S3ClientName,
		OutcomeInformation:   outcomeInfo,
		InstitutionID:        gf.InstitutionID,
		IntellectualObjectID: gf.IntellectualObjectID,
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	// If recording the deletion PREMIS event fails with a 502,
	// then we won't be able to change the GenericFile state to "D".
	// 502s occur sporadically when Pharos is so busy that Nginx
	// can't forward the request. See https://trello.com/c/pI16xrcD
	// for this particular ticket.
	//
	// TODO: This was a problem in Pharos. It may not be in Registry.
	var resp *network.RegistryResponse
	for i := 0; i < 3; i++ {
		resp = m.Context.RegistryClient.PremisEventSave(event)
		if resp.Response.StatusCode != http.StatusBadGateway {
			break
		}
		time.Sleep(1 * time.Second)
	}
	return resp.Error
}

// markObjectDeleted tells Registry that this object has been deleted in its
// entirety (all files deleted).
func (m *Manager) markObjectDeleted() error {
	// TODO: Add manager.ID. This extra lookup is a temporary measure during the rewrite.
	resp := m.Context.RegistryClient.IntellectualObjectByIdentifier(m.Identifier)
	if resp.Error != nil {
		return resp.Error
	}
	obj := resp.IntellectualObject()
	if obj == nil || obj.ID == 0 {
		return fmt.Errorf("registry returned empty object for identifier '%s'", m.Identifier)
	}
	resp = m.Context.RegistryClient.IntellectualObjectDelete(obj.ID)
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
