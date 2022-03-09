package registry

import (
	"encoding/json"
	"strings"
)

// StorageRecord describes where a GenericFile is stored in
// preservation. Each GenericFile can have multiple StorageRecords.
// See common.Config.BucketAndKeyFor(url) to extract
// provider, bucket and key info from the URL.
type StorageRecord struct {
	GenericFileID int64  `json:"generic_file_id"`
	ID            int64  `json:"id,omitempty"`
	URL           string `json:"url"`
}

// StorageRecordFromJSON creates a StorageRecord object from its
// JSON representation.
func StorageRecordFromJSON(jsonData []byte) (*StorageRecord, error) {
	r := &StorageRecord{}
	err := json.Unmarshal(jsonData, r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// ToJSON converts this StorageRecord to its JSON representation.
func (r *StorageRecord) ToJSON() ([]byte, error) {
	bytes, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// UUID returns the last component of the URL, which should
// always be a UUID. The caller should verify that it is in
// fact a UUID, if the caller is concerned about this.
func (r *StorageRecord) UUID() string {
	parts := strings.Split(r.URL, "/")
	return parts[len(parts)-1]
}
