package service

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
)

type RestorationObject struct {
	// AllFilesRestored will be true when all files have been downloaded
	// from preservation to local disk. When this is true, we are ready
	// to create the bag.
	AllFilesRestored bool `json:"all_files_restored"`

	// BagDeletedAt describes when the local copy of the bag was deleted.
	// This should happen after the bag has been uploaded to the depsitor's
	// restoration bucket.
	BagDeletedAt time.Time `json:"bag_deleted_at"`

	// BagItProfileIdentifier is the identifier of the BagItProfile used
	// to build this bag when it was deposited.
	BagItProfileIdentifier string

	// BagValidatedAt describes when the restored bag was validated.
	BagValidatedAt time.Time `json:"bag_validated_at"`

	// ETag is the etag of the restored bag that we pushed into the depositor's
	// restoration bucket.
	ETag string `json:"etag"`

	// ErrorMessage describes the error that prevented this restoration from
	// completing.
	ErrorMessage string `json:"error_message"`

	// FileSize is the size of the file to be restored, or the sum of all the
	// file sizes in the bag to be restored. Note that when restoring a bag,
	// the final restored size will be larger than FileSize because of
	// manifests, tag manifests, and tar headers. This info should come from
	// GenericFile.Size or IntellectualObject.FileSize.
	FileSize int64

	// Identifier is the identifier of the IntellectionObject or GenericFile
	// (from Pharos) to be restored.
	Identifier string `json:"identifier"`

	// RestorationSource describes whether the item being restored is from
	// S3 or Glacier. S3 includes any AWS or Wasabi S3 bucket. Glacier
	// includes any Glacier or Glacier Deep Archive bucket. Items in S3
	// can be restored directly. Those in Glacier must first be moved from
	// the Glacier vault into the S3 bucket of the same name.
	//
	// Valid values for this field are constants.RestorationSourceGlacier
	// and constants.RestorationSourceS3
	RestorationSource string `json:"restoration_source"`

	// RestorationTarget is the name of the depositor's bucket to which
	// the bag should be restored.
	RestorationTarget string `json:"restoration_target"`

	// RestorationType will be either constants.RestorationTypeFile or
	// constants.RestorationTypeObject. Single file restorations require
	// only a single be copied from preservation to the depositor's restoration
	// bucket. Object restorations require downloading and bagging all of the
	// object's files before copying to the restoration bucket.
	RestorationType string `json:"restoration_type"`

	// RestoredAt describes when the restored bag was copied to the depositor's
	// restoration bucket.
	RestoredAt time.Time `json:"restored_at"`

	// URL is the URL of the restored bag in the depositor's restoration
	// bucket.
	URL string `json:"url"`

	// TODO: Delete these if S3 copy works
	DownloadDir string `json:"download_dir"`
	PathToBag   string `json:"path_to_bag"`
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

// ObjName returns the name of the IntellectualObject without the institution
// identifier prefix.
func (obj *RestorationObject) ObjName() (string, error) {
	parts := strings.SplitN(obj.Identifier, "/", 2)
	if len(parts) < 2 {
		return "", fmt.Errorf("Invalid object identifier '%s': missing institution prefix", obj.Identifier)
	}
	return parts[1], nil
}

// BagItProfile returns the short name of the profile to use when restoring
// the bag. This will be either constants.BagItProfileDefault or
// constants.BagItProfileBTR.
func (obj *RestorationObject) BagItProfile() string {
	if obj.BagItProfileIdentifier == constants.DefaultProfileIdentifier {
		return constants.BagItProfileDefault
	}
	return constants.BagItProfileBTR
}

// ManifestAlgorithms describes which digest algorithms to use for
// manifests and tag manifests when restoring a bag.
func (obj *RestorationObject) ManifestAlgorithms() []string {
	if obj.BagItProfile() == constants.BagItProfileDefault {
		return constants.APTrustRestorationAlgorithms
	}
	return constants.BTRRestorationAlgorithms
}
