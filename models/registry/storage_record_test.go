package registry_test

import (
	"testing"

	"github.com/APTrust/preservation-services/models/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var r = &registry.StorageRecord{
	GenericFileID: 999,
	ID:            5432,
	URL:           "https://example.com/preservation/homer.simpson",
}

func TestStorageRecordFromJson(t *testing.T) {
	record, err := registry.StorageRecordFromJSON([]byte(recordJson))
	require.Nil(t, err)
	assert.Equal(t, r, record)
}

func TestStorageRecordToJson(t *testing.T) {
	actualJson, err := r.ToJSON()
	require.Nil(t, err)
	assert.Equal(t, recordJson, string(actualJson))
}

func TestStorageRecordUUID(t *testing.T) {
	r := &registry.StorageRecord{
		URL: "https://example.com/preservation/727800d8-fa5b-4fe8-82c8-78d5dc7bd195",
	}
	assert.Equal(t, "727800d8-fa5b-4fe8-82c8-78d5dc7bd195", r.UUID())
}

var recordJson = `{"generic_file_id":999,"id":5432,"url":"https://example.com/preservation/homer.simpson"}`
