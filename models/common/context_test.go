package common_test

import (
	ctx "context"
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var bucket = "" // set below in uploadTestBag
var key = "institutions.json"
var size = 1161

func uploadTestBag(t *testing.T, context *common.Context) {
	bucket = context.Config.StagingBucket
	filePath := testutil.PathToRegistryFixture("institutions.json")
	client := context.S3Clients[constants.StorageProviderAWS]
	uploadInfo, err := client.FPutObject(
		ctx.Background(),
		bucket,
		key,
		filePath,
		minio.PutObjectOptions{},
	)
	require.Nil(t, err)
	require.EqualValues(t, size, uploadInfo.Size)
}

func TestS3ObjectGet(t *testing.T) {
	context := common.NewContext()
	uploadTestBag(t, context)

	minioObj, err := context.S3GetObject(
		constants.StorageProviderAWS,
		bucket,
		key,
	)
	defer minioObj.Close()
	require.Nil(t, err)
	require.NotNil(t, minioObj)

	objInfo, err := minioObj.Stat()
	require.Nil(t, err)
	require.NotNil(t, objInfo)
	assert.EqualValues(t, size, objInfo.Size)
}

func TestS3StatGet(t *testing.T) {
	context := common.NewContext()
	uploadTestBag(t, context)

	stats, err := context.S3StatObject(
		constants.StorageProviderAWS,
		bucket,
		key,
	)
	require.Nil(t, err)
	require.NotNil(t, stats)
	assert.EqualValues(t, size, stats.Size)
}
