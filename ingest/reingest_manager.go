package ingest

import (
	"net/url"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
)

// ReingestManager checks Registry to see whether the object we're ingesting
// has ever been ingested before. If so, it checks each file in the new
// object to see if it's new (i.e. never been ingested before) or an updated
// version of a previously ingested file.
//
// The ReingestManager updates internal metadata on the IngestObject files,
// including the following:
//
// 1. If a file in the ingest package has already been ingested, and the
// size and checksum match what's in Registry, it marks the file as "no need
// to store," since we already have a copy of this file in preservation
// storage.
//
// 2. If a file has already been ingested but the checksum differs, we
// set the file's UUID to match the UUID of the previously ingested version.
// This allows us to overwrite the old version with the new in preservation
// storage. We do not want to wind up with multiple copies (multiple UUIDs)
// of the same object in storage, because our contract with depositors is
// to keep only the most recent version. Registry can't even track other
// versions, so we'd be paying for the storage of orphaned files. The basic
// rule is one UUID per GenericFile identifier, forever.
//
// 3. If a file is new (i.e. its identifier does not exist in Registry), the
// ReingestManager does not alter any of its metadata. It will follow the
// normal ingest process.
//
// The ReingestManager's sole task is to adjust the internal metadata of
// IntellectualObject and GenericFile records so that subsequent workers in
// the ingest process know how to process the object and files. This does not
// alter new objects at all, only object that have been previously ingested.
type ReingestManager struct {
	Base
}

// NewReingestManager creates a new ReingestManager.
func NewReingestManager(context *common.Context, workItemID int64, ingestObject *service.IngestObject) *ReingestManager {
	return &ReingestManager{
		Base{
			Context:      context,
			IngestObject: ingestObject,
			WorkItemID:   workItemID,
		},
	}
}

// Run checks to see whether we've ingested a version of this
// object in the past. If so, it checks the files in the new version against
// the files in the registry to see what has changed.
//
// Returns 1 if this object has been previously ingested, zero if not.
// Returns errors if any part of the processing failed.
func (r *ReingestManager) Run() (isReingest int, errors []*service.ProcessingError) {
	isReingest = 0
	obj, err := r.GetExistingObject()
	if err == nil {
		if obj != nil {
			isReingest = 1
			saveErr := r.FlagObjectAsReingest(obj)
			if saveErr != nil {
				errors = append(errors, r.Error(r.IngestObject.Identifier(), saveErr, true))
				return isReingest, errors
			}

			// ProcessFiles can really hammer Registry if we have
			// a lot of files. Call this only if it really is a reingest.
			_, errors = r.ProcessFiles()
		}
		if len(errors) > 0 {
			return isReingest, errors
		}
	} else {
		errors = append(errors, r.Error(r.IngestObject.Identifier(), err, false))
	}
	if isReingest == 1 {
		r.Context.Logger.Infof("WorkItem %d (%s) is a reingest", r.WorkItemID, r.IngestObject.Identifier())
	} else {
		r.Context.Logger.Infof("WorkItem %d (%s) is not a reingest", r.WorkItemID, r.IngestObject.Identifier())
	}
	return isReingest, errors
}

// GetExistingObject returns true if the IngestObject has been
// previously ingested. Returns an error if it can't get info from Registry.
func (r *ReingestManager) GetExistingObject() (*registry.IntellectualObject, error) {
	resp := r.Context.RegistryClient.IntellectualObjectByIdentifier(r.IngestObject.Identifier())
	if resp.ObjectNotFound() {
		return nil, nil
	} else if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.IntellectualObject(), nil
}

// ProcessFiles checks each of the IngestFiles against existing records in
// Registry. If it finds an existing record, it sets the UUID of the IngestFile
// to match the one in Registry, and compares checksums to see if we need to
// re-copy the file into preservation storage.
func (r *ReingestManager) ProcessFiles() (int, []*service.ProcessingError) {
	processFile := func(ingestFile *service.IngestFile) (errors []*service.ProcessingError) {
		resp := r.Context.RegistryClient.GenericFileByIdentifier(ingestFile.Identifier())
		// Most files will be new files, so we expect lots of 404s.
		// If we get 404, we do not need to flag changes for this file
		// because it's new.
		if resp.ObjectNotFound() {
			return errors
		}
		if resp.Error != nil {
			return append(errors, r.Error(ingestFile.Identifier(), resp.Error, false))
		}
		registryFile := resp.GenericFile()
		if registryFile != nil {
			ingestFile.ID = registryFile.ID
			r.FlagChanges(ingestFile, registryFile)
		}

		// Get a list of preservation storage URLs for this file
		// that Registry already knows about. We'll need this later
		// in the record phase. Registry has a unique constraint on
		// these storage URLs, and if we try to re-save a StorageRecord
		// whose URL is already in the DB, we'll get a unique constraint
		// error.
		params := url.Values{}
		params.Add("generic_file_identifier", ingestFile.Identifier())
		resp = r.Context.RegistryClient.StorageRecordList(params)
		if resp.Error != nil {
			return append(errors, r.Error(ingestFile.Identifier(), resp.Error, false))
		}
		for _, sr := range resp.StorageRecords() {
			if !ingestFile.HasRegistryURL(sr.URL) {
				ingestFile.RegistryURLs = append(ingestFile.RegistryURLs, sr.URL)
			}
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

// ProcessFile requests a GenericFile object from Registry. If Registry returns
// a record, we know the file has been ingested before. We compare the checksum
// of the Registry version with the checksum of the version we're about to
// ingest. If they match, we flag our copy of the GenericFile as "no need to
// store."
//
// If the file exists in Registry, this method updates our local GenericFile
// UUID to match the Registry file's UUID, so that when we do store the
// file, we overwrite the previous version.
//
// This returns true if it updated the IngestFile record Redis. It returns
// an error if it has trouble communicating with Registry or Redis.
func (r *ReingestManager) ProcessFile(ingestFile *service.IngestFile) (bool, error) {
	updatedInRedis := false
	resp := r.Context.RegistryClient.GenericFileByIdentifier(ingestFile.Identifier())
	if resp.Error != nil {
		return updatedInRedis, resp.Error
	}
	registryFile := resp.GenericFile()
	if registryFile != nil {
		r.FlagChanges(ingestFile, registryFile)
		err := r.IngestFileSave(ingestFile)
		if err != nil {
			return updatedInRedis, err
		}
		updatedInRedis = true
	}
	return updatedInRedis, nil
}

// FlagChanges checks to see if the checksums on the IngestFile match the
// checksums on Registry' GenericFile. If not, this flags the file as needing
// to be re-copied to preservation storage. If checksums match, this flags
// the file as not needing to be copied.
//
// This returns a boolean indicating whether the file has changed since last
// ingest. It returns an error if it has trouble getting info from Registry.
func (r *ReingestManager) FlagChanges(ingestFile *service.IngestFile, registryFile *registry.GenericFile) (bool, error) {
	fileChanged := false
	params := url.Values{}
	params.Add("generic_file_identifier", ingestFile.Identifier())
	params.Add("sort", "datetime__desc")

	resp := r.Context.RegistryClient.ChecksumList(params)
	if resp.Error != nil {
		return fileChanged, resp.Error
	}

	r.SetStorageOption(ingestFile, registryFile)

	newestChecksumsFromRegistry := r.GetNewest(resp.Checksums())
	if r.ChecksumChanged(ingestFile, newestChecksumsFromRegistry) {
		fileChanged = true
		r.FlagForUpdate(ingestFile, registryFile)
	} else {
		r.FlagUnchanged(ingestFile, registryFile)
	}

	return fileChanged, nil
}

// Returns a map of the most recent checksum of each type. The key is the
// algorithm name (e.g. "md5", "sha256"). The value is the checksum itself.
func (r *ReingestManager) GetNewest(checksums []*registry.Checksum) map[string]*registry.Checksum {
	// Registry is supposed to return these records descending datetime order,
	// but let's make sure.
	newest := make(map[string]*registry.Checksum, 0)
	for _, cs := range checksums {
		if existing, ok := newest[cs.Algorithm]; !ok {
			newest[cs.Algorithm] = cs
		} else if cs.Algorithm == existing.Algorithm && cs.DateTime.After(existing.DateTime) {
			newest[cs.Algorithm] = cs
		}
	}
	return newest
}

// Compare checksums of IngestFile to checksums in Registry and return true if
// a checksum has changed. We compare checksums in preferred order: sha512,
// then sha256, then md5. As of early 2020, Registry has only sha256 and md5,
// though that should change in the future.
//
// This returns true if the file has changed since it was last ingested.
func (r *ReingestManager) ChecksumChanged(ingestFile *service.IngestFile, registryChecksums map[string]*registry.Checksum) bool {
	changed := false
	for _, alg := range constants.PreferredAlgsInOrder {
		if registryChecksum, ok := registryChecksums[alg]; ok {
			ingestChecksum := ingestFile.GetChecksum(constants.SourceIngest, alg)
			if ingestChecksum != nil && ingestChecksum.Digest != registryChecksum.Digest {
				r.Context.Logger.Infof("Digest %s changed for file %s", alg, ingestFile.Identifier())
				changed = true
				break
			}
		}
	}
	return changed
}

// Force the StorageOption of the new IngestFile to match the StorageOption of
// the existing file (unless the existed file is marked as deleted). We do this
// to avoid having multiple versions of a file stored in different places. This
// is a documented issue, and depositors know about it if they've read the docs.
//
// In short, an object stays in the StorageOption you specified upon first
// ingest, even if subsequent ingests say it should go somewhere else. The only
// way to change the storage option of an existing object is to completely
// delete it, then reingest it.
func (r *ReingestManager) SetStorageOption(ingestFile *service.IngestFile, registryFile *registry.GenericFile) {
	if registryFile.State == "A" && ingestFile.StorageOption != registryFile.StorageOption {
		r.Context.Logger.Infof("Changing StorageOption of %s from %s to %s to match previously ingested version.", ingestFile.Identifier(), ingestFile.StorageOption, registryFile.StorageOption)
		ingestFile.StorageOption = registryFile.StorageOption
		r.IngestObject.StorageOption = registryFile.StorageOption
	}
}

// FlagForUpdate marks an IngestFile as needing to be saved, and sets the
// UUID to the existing UUID in Registry.
func (r *ReingestManager) FlagForUpdate(ingestFile *service.IngestFile, registryFile *registry.GenericFile) {
	ingestFile.IsReingest = true
	ingestFile.NeedsSave = true
	ingestFile.UUID = registryFile.UUID
}

// FlagUnchanged marks an IngestFile as NOT needing to be saved, and sets the
// UUID to the existing UUID in Registry.
func (r *ReingestManager) FlagUnchanged(ingestFile *service.IngestFile, registryFile *registry.GenericFile) {
	ingestFile.IsReingest = true
	ingestFile.NeedsSave = false
	ingestFile.UUID = registryFile.UUID
}

func (r *ReingestManager) FlagObjectAsReingest(obj *registry.IntellectualObject) error {
	r.IngestObject.ID = obj.ID
	r.IngestObject.IsReingest = true
	return r.IngestObjectSave()
}
