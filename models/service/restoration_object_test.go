package service_test

import (
	"testing"

	"github.com/APTrust/preservation-services/constants"
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
	assert.Equal(t, expectedObj.AllFilesRestored, obj.AllFilesRestored)
	assert.Equal(t, expectedObj.ErrorMessage, obj.ErrorMessage)
	assert.Equal(t, expectedObj.Identifier, obj.Identifier)
	assert.Equal(t, expectedObj.RestoredAt, obj.RestoredAt)
	assert.Equal(t, expectedObj.URL, obj.URL)
}

func TestRestorationObjToJSON(t *testing.T) {
	obj := testutil.GetRestorationObject()
	data, err := obj.ToJSON()
	assert.Nil(t, err)
	assert.Equal(t, RestorationObjectJSON, data)
}

func TestObjName(t *testing.T) {
	obj := &service.RestorationObject{
		Identifier: "test.edu/sample-bag",
	}
	ident, err := obj.ObjName()
	require.Nil(t, err)
	assert.Equal(t, "sample-bag", ident)

	obj.Identifier = "sample-bag"
	ident, err = obj.ObjName()
	require.NotNil(t, err)
	assert.Equal(t, "", ident)
}

func TestRestorationObj_BagItProfile(t *testing.T) {
	obj := &service.RestorationObject{
		BagItProfileIdentifier: constants.DefaultProfileIdentifier,
	}
	assert.Equal(t, constants.BagItProfileDefault, obj.BagItProfile())

	obj = &service.RestorationObject{
		BagItProfileIdentifier: constants.BTRProfileIdentifier,
	}
	assert.Equal(t, constants.BagItProfileBTR, obj.BagItProfile())
}

func TestRestorationObj_ManifestAlgorithms(t *testing.T) {
	obj := &service.RestorationObject{
		BagItProfileIdentifier: constants.DefaultProfileIdentifier,
	}
	assert.Equal(t, constants.APTrustRestorationAlgorithms, obj.ManifestAlgorithms())

	obj = &service.RestorationObject{
		BagItProfileIdentifier: constants.BTRProfileIdentifier,
	}
	assert.Equal(t, constants.BTRRestorationAlgorithms, obj.ManifestAlgorithms())
}

const RestorationObjectJSON = `{"all_files_restored":true,"BagItProfileIdentifier":"https://raw.githubusercontent.com/APTrust/preservation-services/master/profiles/aptrust-v2.3.json","error_message":"No error","identifier":"test.edu/bag-name.tar","item_id":111222333444,"object_size":333000,"restoration_source":"s3","restoration_target":"aptrust.restore.test.edu","restoration_type":"object","restored_at":"1904-06-16T15:04:05Z","url":"https://s3.example.com/restore-bucket/bag-name.tar"}`
