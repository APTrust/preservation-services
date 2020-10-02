// +build e2e

package e2e_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test ensures that the file and metadata that storageRecord point to
// are actually present in the correct preservation buckets.
//
// This is called from testStorageRecords. Param gf is the GenericFile
// record retrieved from Pharos. Param storageRecord is the specific record
// to test.
func testS3File(storageRecord *registry.StorageRecord, gf *registry.GenericFile) {
	preservationBucket, key, err := ctx.Context.Config.BucketAndKeyFor(storageRecord.URL)
	require.Nil(ctx.T, err)
	require.True(ctx.T, util.LooksLikeUUID(key))

	objInfo, err := ctx.Context.S3StatObject(
		preservationBucket.Provider,
		preservationBucket.Bucket,
		key,
	)
	require.Nil(ctx.T, err, gf.Identifier)

	md5 := gf.GetLatestChecksum(constants.AlgMd5)
	sha256 := gf.GetLatestChecksum(constants.AlgSha256)
	require.NotNil(ctx.T, md5, gf.Identifier)
	require.NotNil(ctx.T, sha256, gf.Identifier)

	// ---- DEBUG ----
	// ctx.Context.Logger.Info(gf.Identifier)
	// ctx.Context.Logger.Info(objInfo)
	// ctx.Context.Logger.Info("UserMetadata...")
	// for k, v := range objInfo.UserMetadata {
	// 	ctx.Context.Logger.Infof("%s = %s", k, v)
	// }
	// ctx.Context.Logger.Info("Metadata...")
	// for k, v := range objInfo.Metadata {
	// 	ctx.Context.Logger.Infof("%s = %s", k, v)
	// }
	// ---- DEBUG ----

	// Note that Minio capitalizes our UserMetadata tags.
	assert.Equal(ctx.T, gf.InstitutionIdentifier(), objInfo.UserMetadata["Institution"])
	assert.Equal(ctx.T, gf.IntellectualObjectIdentifier, objInfo.UserMetadata["Bag"])
	assert.Equal(ctx.T, gf.PathInBag(), objInfo.UserMetadata["Bagpath"])
	assert.Equal(ctx.T, md5.Digest, objInfo.UserMetadata["Md5"])
	assert.Equal(ctx.T, sha256.Digest, objInfo.UserMetadata["Sha256"])
}

// After ingest and reingest, the staging bucket should be empty.
func testS3Cleanup() {
	client := ctx.Context.S3Clients[constants.StorageProviderAWS]
	require.NotNil(ctx.T, client)

	bucket := ctx.Context.Config.StagingBucket
	doneCh := make(chan struct{})
	defer close(doneCh)

	for objInfo := range client.ListObjects(bucket, "", true, doneCh) {
		assert.Nil(ctx.T, objInfo, "%s was not deleted from staging bucket", objInfo.Key)
	}
}
