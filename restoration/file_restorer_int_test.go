//go:build integration
// +build integration

package restoration_test

import (
	ctx "context"
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/restoration"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This generic file is loaded as part of the Registry integration
// text fixture set.
var gfIdentifier = "test.edu/apt-test-restore/data/sample.xml"
var gfWorkItemID = 56338

// Size, in bytes, of our sample file
var gfSize = int64(23972)

func TestNewFileRestorer(t *testing.T) {
	restorer := restoration.NewBagRestorer(
		common.NewContext(),
		gfWorkItemID,
		getRestorationObject(gfIdentifier))
	require.NotNil(t, restorer)
	require.NotNil(t, restorer.Context)
	assert.Equal(t, gfWorkItemID, restorer.WorkItemID)
	assert.Equal(t, gfIdentifier, restorer.RestorationObject.Identifier)
}

func TestFileRestorer_Run(t *testing.T) {
	context := common.NewContext()
	setup(t, context) // setup is defined in bag_restorer_int_test.go
	restObj := getRestorationObject(gfIdentifier)
	restorer := restoration.NewFileRestorer(context, gfWorkItemID, restObj)
	fileCount, errors := restorer.Run()
	assert.Equal(t, 1, fileCount)
	assert.Empty(t, errors)
	testRestoredFile(t, context, restObj)
}

func testRestoredFile(t *testing.T, context *common.Context, restObj *service.RestorationObject) {
	assert.NotEmpty(t, restObj.URL)
	objInfo, err := context.S3Clients[constants.StorageProviderAWS].StatObject(
		ctx.Background(),
		restObj.RestorationTarget,
		gfIdentifier,
		minio.StatObjectOptions{})
	require.Nil(t, err)
	assert.Equal(t, gfSize, objInfo.Size)
}
