package service_test

import (
	"testing"

	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestorationObjFromJson(t *testing.T) {
	expectedObj := testutil.GetRestorationObject()
	obj, err := service.RestorationObjectFromJSON(RestorationObjectJSON)
	require.Nil(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, expectedObj.AllFilesDownloaded, obj.AllFilesDownloaded)
	assert.Equal(t, expectedObj.BagDeletedAt, obj.BagDeletedAt)
	assert.Equal(t, expectedObj.BagValidatedAt, obj.BagValidatedAt)
	assert.Equal(t, expectedObj.DownloadDir, obj.DownloadDir)
	assert.Equal(t, expectedObj.ETag, obj.ETag)
	assert.Equal(t, expectedObj.ErrorMessage, obj.ErrorMessage)
	assert.Equal(t, expectedObj.Identifier, obj.Identifier)
	assert.Equal(t, expectedObj.PathToBag, obj.PathToBag)
	assert.Equal(t, expectedObj.RestoredAt, obj.RestoredAt)
	assert.Equal(t, expectedObj.RestoredBagSize, obj.RestoredBagSize)
	assert.Equal(t, expectedObj.URL, obj.URL)
}

func TestRestorationObjToJSON(t *testing.T) {
	obj := testutil.GetRestorationObject()
	data, err := obj.ToJSON()
	assert.Nil(t, err)
	assert.Equal(t, RestorationObjectJSON, data)
}

const RestorationObjectJSON = `{"all_files_downloaded":true,"bag_deleted_at":"1904-06-16T15:04:05Z","bag_validated_at":"1904-06-16T15:04:05Z","download_dir":"/mnt/data","etag":"1234567890","error_message":"No error","identifier":"test.edu/bag-name.tar","path_to_bag":"/mnt/data/restore/test.edu/bag-name.tar","restored_at":"1904-06-16T15:04:05Z","restored_bag_size":9999,"url":"https://s3.example.com/restore-bucket/bag-name.tar"}`
