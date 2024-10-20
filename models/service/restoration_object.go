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

	// BagItProfileIdentifier is the identifier of the BagItProfile used
	// to build this bag when it was deposited.
	BagItProfileIdentifier string

	// ErrorMessage describes the error that prevented this restoration from
	// completing.
	ErrorMessage string `json:"error_message"`

	// Identifier is the identifier of the IntellectionObject or GenericFile
	// (from Registry) to be restored.
	Identifier string `json:"identifier"`

	// ItemID is the ID of the IntellectualObject or GenericFile to be restored.
	ItemID int64 `json:"item_id"`

	// ObjectSize is the size of the bag or file to be restored. For bags,
	// this is actually the size of the payload. The final bag will be somewhat
	// larger because it will include manifests and tag files in addition to
	// the payload. The final bag may be ~ 1% - 10% larger than ObjectSize.
	ObjectSize int64 `json:"object_size"`

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
	// Why is the profile URL inconsistent?
	if obj.BagItProfileIdentifier == constants.BTRProfileIdentifier || strings.Contains(obj.BagItProfileIdentifier, "btr-bagit-profile") {
		return constants.BagItProfileBTR
	}
	return constants.BagItProfileDefault
}

// ManifestAlgorithms describes which digest algorithms to use for
// manifests and tag manifests when restoring a bag.
func (obj *RestorationObject) ManifestAlgorithms() []string {
	if obj.BagItProfile() == constants.BagItProfileDefault {
		return constants.APTrustRestorationAlgorithms
	}
	return constants.BTRRestorationAlgorithms
}
