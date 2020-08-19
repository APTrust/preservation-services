package service

import (
	"encoding/json"
	"time"
)

type RestorationObject struct {
	// AllFilesDownloaded will be true when all files have been downloaded
	// from preservation to local disk. When this is true, we are ready
	// to create the bag.
	AllFilesDownloaded bool `json:"all_files_downloaded"`

	// BagDeletedAt describes when the local copy of the bag was deleted.
	// This should happen after the bag has been uploaded to the depsitor's
	// restoration bucket.
	BagDeletedAt time.Time `json:"bag_deleted_at"`

	// BagValidatedAt describes when the restored bag was validated.
	BagValidatedAt time.Time `json:"bag_validated_at"`

	// DownloadDir is the path the directory (on local disk) to which
	// we've downloaded files for restoration.
	DownloadDir string `json:"download_dir"`

	// ETag is the etag of the restored bag that we pushed into the depositor's
	// restoration bucket.
	ETag string `json:"etag"`

	// ErrorMessage describes the error that prevented this restoration from
	// completing.
	ErrorMessage string `json:"error_message"`

	// Identifier is the identifier of the IntellectionObject or GenericFile
	// (from Pharos) to be restored.
	Identifier string `json:"identifier"`

	// PathToBag is the path the restored bag on local disk.
	PathToBag string `json:"path_to_bag"`

	// RestorationSource describes whether the item being restored is from
	// S3 or Glacier. S3 includes any AWS or Wasabi S3 bucket. Glacier
	// includes any Glacier or Glacier Deep Archive bucket. Items in S3
	// can be restored directly. Those in Glacier must first be moved from
	// the Glacier vault into the S3 bucket of the same name.
	//
	// Valid values for this field are constants.RestorationSourceGlacier
	// and constants.RestorationSourceS3
	RestorationSource string

	// RestorationType will be either constants.RestorationTypeFile or
	// constants.RestorationTypeObject. Single file restorations require
	// only a single be copied from preservation to the depositor's restoration
	// bucket. Object restorations require downloading and bagging all of the
	// object's files before copying to the restoration bucket.
	RestorationType string

	// RestoredAt describes when the restored bag was copied to the depositor's
	// restoration bucket.
	RestoredAt time.Time `json:"restored_at"`

	// RestoredBagSize is the size (in bytes) of the restored bag. This will be
	// different from the original bag size because we include Premis events
	// in the restored bag. The restored bag may include files added or updated
	// since the initial ingest, and will not include files deleted after the
	// initial ingest.
	RestoredBagSize int64 `json:"restored_bag_size"`

	// URL is the URL of the restored bag in the depositor's restoration
	// bucket.
	URL string `json:"url"`
}

// RestorationObjectFromJSON converts the JSON representation of a
// RestorationObject to an actual object.
func RestorationObjectFromJSON(jsonData string) (*RestorationObject, error) {
	obj := &RestorationObject{}
	err := json.Unmarshal([]byte(jsonData), obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// ToJSON converts this object to its JSON representation.
func (obj *RestorationObject) ToJSON() (string, error) {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
