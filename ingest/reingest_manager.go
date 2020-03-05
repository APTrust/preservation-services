package ingest

import (
	"net/url"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
)

// ReingestManager checks Pharos to see whether the object we're ingesting
// has ever been ingested before. If so, it checks each file in the new
// object to see if it's new (i.e. never been ingested before) or an updated
// version of a previously ingested file.
//
// The ReingestManager updates internal metadata on the IngestObject files,
// including the following:
//
// 1. If a file in the ingest package has already been ingested, and the
// size and checksum match what's in Pharos, it marks the file as "no need
// to store," since we already have a copy of this file in preservation
// storage.
//
// 2. If a file has already been ingested but the checksum differs, we
// set the file's UUID to match the UUID of the previously ingested version.
// This allows us to overwrite the old version with the new in preservation
// storage. We do not want to wind up with multiple copies (multiple UUIDs)
// of the same object in storage, because our contract with depositors is
// to keep only the most recent version. Pharos can't even track other
// versions, so we'd be paying for the storage of orphaned files. The basic
// rule is one UUID per GenericFile identifier, forever.
//
// 3. If a file is new (i.e. its identifier does not exist in Pharos), the
// ReingestManager does not alter any of its metadata. It will follow the
// normal ingest process.
//
// The ReingestManager's sole task is to adjust the internal metadata of
// IntellectualObject and GenericFile records so that subsequent workers in
// the ingest process know how to process the object and files. This does not
// alter new objects at all, only object that have been previously ingested.
type ReingestManager struct {
	Context      *common.Context
	IngestObject *service.IngestObject
	WorkItemId   int
}

// NewReingestManager creates a new ReingestManager.
func NewReingestManager(context *common.Context, workItemId int, ingestObject *service.IngestObject) *ReingestManager {
	return &ReingestManager{
		Context:      context,
		IngestObject: ingestObject,
		WorkItemId:   workItemId,
	}
}

// ProcessObject checks to see whether we've ingested a version of this
// object in the past. If so, it checks the files in the new version against
// the files in the registry to see what has changed.
//
// Returns true if this object has been previously ingested. Returns an error
// if any part of the processing failed.
func (r *ReingestManager) ProcessObject() (bool, error) {
	var err error
	isReingest := false
	if isReingest, err = r.ObjectWasPreviouslyIngested(); err == nil {
		err = r.ProcessFiles()
		if err != nil {
			return isReingest, err
		}
	}
	return isReingest, err
}

// ObjectWasPreviouslyIngested returns true if the IngestObject has been
// previously ingested. Returns an error if it can't get info from Pharos.
func (r *ReingestManager) ObjectWasPreviouslyIngested() (bool, error) {
	resp := r.Context.PharosClient.IntellectualObjectGet(r.IngestObject.Identifier())
	if resp.ObjectNotFound() {
		return false, nil
	} else if resp.Error != nil {
		return false, resp.Error
	}
	obj := resp.IntellectualObject()
	return obj != nil, nil
}

// ProcessFiles checks each of the IngestFiles against existing records in
// Pharos. If it finds an existing record, it sets the UUID of the IngestFile
// to match the one in Pharos, and compares checksums to see if we need to
// re-copy the file into preservation storage.
func (r *ReingestManager) ProcessFiles() error {
	batchSize := 500
	nextOffset := uint64(0)
	for {
		fileMap, nextOffset, err := r.Context.RedisClient.GetBatchOfFileKeys(
			r.WorkItemId,
			nextOffset,
			int64(batchSize))
		if err != nil {
			return err
		}
		for _, ingestFile := range fileMap {
			updatedInRedis, err := r.ProcessFile(ingestFile)
			if err != nil {
				return err
			}
			if updatedInRedis {
				r.Context.Logger.Infof("Updated %s in Redis", ingestFile.Identifier())
			}
		}
		if nextOffset == 0 {
			break
		}
	}
	return nil
}

// ProcessFile requests a GenericFile object from Pharos. If Pharos returns
// a record, we know the file has been ingested before. We compare the checksum
// of the Pharos version with the checksum of the version we're about to
// ingest. If they match, we flag our copy of the GenericFile as "no need to
// store."
//
// If the file exists in Pharos, this method updates our local GenericFile
// UUID to match the Pharos file's UUID, so that when we do store the
// file, we overwrite the previous version.
//
// This returns true if it updated the IngestFile record Redis. It returns
// an error if it has trouble communicating with Pharos or Redis.
func (r *ReingestManager) ProcessFile(ingestFile *service.IngestFile) (bool, error) {
	updatedInRedis := false
	resp := r.Context.PharosClient.GenericFileGet(ingestFile.Identifier())
	if resp.Error != nil {
		return updatedInRedis, resp.Error
	}
	pharosFile := resp.GenericFile()
	if pharosFile != nil {
		r.CompareFiles(ingestFile, pharosFile)
		err := r.Context.RedisClient.IngestFileSave(r.WorkItemId, ingestFile)
		if err != nil {
			return updatedInRedis, err
		}
		updatedInRedis = true
	}
	return updatedInRedis, nil
}

// CompareFiles checks to see if the checksums on the IngestFile match the
// checksums on Pharos' GenericFile. If not, this flags the file as needing
// to be re-copied to preservation storage. If checksums match, this flags
// the file as not needing to be copied.
//
// This returns a boolean indicating whether the file has changed since last
// ingest. It returns an error if it has trouble getting info from Pharos.
func (r *ReingestManager) CompareFiles(ingestFile *service.IngestFile, pharosFile *registry.GenericFile) (bool, error) {
	fileChanged := false
	params := url.Values{}
	params.Add("generic_file_identifier", ingestFile.Identifier())
	params.Add("sort", "datetime DESC")

	resp := r.Context.PharosClient.ChecksumList(params)
	if resp.Error != nil {
		return fileChanged, resp.Error
	}

	r.SetStorageOption(ingestFile, pharosFile)

	newestChecksumsFromPharos := r.GetNewest(resp.Checksums())
	if r.ChecksumChanged(ingestFile, newestChecksumsFromPharos) {
		fileChanged = true
		r.FlagForUpdate(ingestFile, pharosFile)
	} else {
		r.FlagUnchanged(ingestFile, pharosFile)
	}

	return fileChanged, nil
}

// Returns a map of the most recent checksum of each type. The key is the
// algorithm name (e.g. "md5", "sha256"). The value is the checksum itself.
func (r *ReingestManager) GetNewest(checksums []*registry.Checksum) map[string]*registry.Checksum {
	// Pharos is supposed to return these records descending datetime order,
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

// Compare checksums of IngestFile to checksums in Pharos and return true if
// a checksum has changed. We compare checksums in preferred order: sha512,
// then sha256, then md5. As of early 2020, Pharos has only sha256 and md5,
// though that should change in the future.
//
// This returns true if the file has changed since it was last ingested.
func (r *ReingestManager) ChecksumChanged(ingestFile *service.IngestFile, pharosChecksums map[string]*registry.Checksum) bool {
	changed := false
	for _, alg := range constants.PreferredAlgsInOrder {
		if pharosChecksum, ok := pharosChecksums[alg]; ok {
			ingestChecksum := ingestFile.GetChecksum(constants.SourceIngest, alg)
			if ingestChecksum != nil && ingestChecksum.Digest != pharosChecksum.Digest {
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
func (r *ReingestManager) SetStorageOption(ingestFile *service.IngestFile, pharosFile *registry.GenericFile) {
	if pharosFile.State == "A" {
		ingestFile.StorageOption = pharosFile.StorageOption
	}
}

// FlagForUpdate marks an IngestFile as needing to be saved, and sets the
// UUID to the existing UUID in Pharos.
func (r *ReingestManager) FlagForUpdate(ingestFile *service.IngestFile, pharosFile *registry.GenericFile) {
	ingestFile.NeedsSave = true
	ingestFile.UUID = pharosFile.UUID()
}

// FlagUnchanged marks an IngestFile as NOT needing to be saved, and sets the
// UUID to the existing UUID in Pharos.
func (r *ReingestManager) FlagUnchanged(ingestFile *service.IngestFile, pharosFile *registry.GenericFile) {
	ingestFile.NeedsSave = false
	ingestFile.UUID = pharosFile.UUID()
}
