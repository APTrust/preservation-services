package deletion

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
)

type Manager struct {
	Context    *common.Context
	Identifier string
	ItemType   string
	WorkItemID int
}

func NewManager(context *common.Context, workItemID int, identifier, itemType string) *Manager {
	return &Manager{
		Context:    context,
		Identifier: identifier,
		ItemType:   itemType,
		WorkItemID: workItemID,
	}
}

func (m *Manager) Run() (count int, errors []*service.ProcessingError) {
	if m.ItemType == constants.TypeFile {
		count, errors = m.deleteSingleFile()
	} else {
		count, errors = m.deleteFiles()
	}
	return count, errors
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
				err := m.markFileDeleted(gf)
				if err != nil {
					errors = append(errors, m.Error(gf.Identifier, err, false))
				} else {
					count++
				}
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
		errors = append(errors, m.Error(m.Identifier, err, false))
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
		} else {
			err := m.deleteStorageRecord(provider, bucket, key)
			if err != nil {
				errors = append(errors, m.Error(gf.Identifier, err, false))
			}
		}
	}
	return errors
}

func (m *Manager) deleteStorageRecord(provider, bucket, key string) error {
	client := m.Context.S3Clients[provider]
	if client == nil {
		return fmt.Errorf("No S3 client for provider %s", provider)
	}
	err := client.RemoveObject(bucket, key)

	// We can ignore this message because the item may have been deleted
	// on a prior attempt.
	if strings.Contains(err.Error(), "key does not exist") {
		m.Context.Logger.Warningf("Item %s %s/%s does not exist. May have been deleted in prior run.",
			provider, bucket, key)
		return nil
	}

	if err == nil {
		// START HERE
		// TODO: Add StorageRecordDelete method to PharosClient.
		// TODO: Delete StorageRecord in Pharos.
	}

	// Other errors are permission denied, bucket does not exist, conflict,
	// request limit. These need to be reported.
	return err
}

func (m *Manager) markFileDeleted(gf *registry.GenericFile) error {
	gf.State = constants.StateDeleted
	resp := m.Context.PharosClient.GenericFileSave(gf)
	return resp.Error
}

func (m *Manager) markObjectDeleted() error {
	resp := m.Context.PharosClient.IntellectualObjectGet(m.Identifier)
	if resp.Error != nil {
		return resp.Error
	}
	obj := resp.IntellectualObject()
	if obj == nil {
		return fmt.Errorf("Pharos returned nil for object %s", m.Identifier)
	}
	obj.State = constants.StateDeleted
	resp = m.Context.PharosClient.IntellectualObjectSave(obj)
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
