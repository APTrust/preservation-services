package registry

import (
	"encoding/json"
)

// StorageRecord describes where a GenericFile is stored in
// preservation. Each GenericFile can have multiple StorageRecords.
// See common.Config.ProviderBucketAndKeyFor(url) to extract
// provider, bucket and key info from the URL.
type StorageRecord struct {
	GenericFileID int    `json:"generic_file_id"`
	ID            int    `json:"id,omitempty"`
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
// See also SerializeForPharos.
func (r *StorageRecord) ToJSON() ([]byte, error) {
	bytes, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// SerializeForPharos serializes this record for Pharos. Note that
// Pharos supports only POST/create, not PUT/update for StorageRecords.
// Since Pharos assigns the ID and GenericFileID during creation,
// we only the URL.
func (r *StorageRecord) SerializeForPharos() ([]byte, error) {
	dataStruct := make(map[string]string)
	dataStruct["url"] = r.URL
	return json.Marshal(dataStruct)
}
