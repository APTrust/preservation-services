//go:build e2e
// +build e2e

package e2e_test

import (
	"net/url"
	"strconv"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/e2e"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testFileDeletions() {
	for _, gfIdentifier := range e2e.FilesToDelete {
		resp := ctx.Context.RegistryClient.GenericFileByIdentifier(gfIdentifier)
		require.Nil(ctx.T, resp.Error)
		gf := resp.GenericFile()
		require.NotNil(ctx.T, gf)
		verifyFileDeletion(gf)
		verifyFileDeletionWorkItem(gf)
	}
}

func testObjectDeletions() {
	for _, objIdentifier := range e2e.ObjectsToDelete {
		resp := ctx.Context.RegistryClient.IntellectualObjectByIdentifier(objIdentifier)
		require.Nil(ctx.T, resp.Error)
		obj := resp.IntellectualObject()
		require.NotNil(ctx.T, obj)

		assert.Equal(ctx.T, constants.StateDeleted, obj.State)
		verifyObjectDeletionEvent(obj)
		verifyObjectDeletionWorkItem(obj)

		params := url.Values{}
		params.Add("intellectual_object_id", strconv.FormatInt(obj.ID, 10))
		params.Add("per_page", "200")
		params.Add("page", "1")
		resp = ctx.Context.RegistryClient.GenericFileList(params)
		require.Nil(ctx.T, resp.Error)
		for _, gf := range resp.GenericFiles() {
			verifyFileDeletion(gf)
		}
	}
}

func verifyFileDeletion(gf *registry.GenericFile) {
	assert.Equal(ctx.T, constants.StateDeleted, gf.State)
	verifyFileDeletionEvent(gf)
	verifyStorageRecordsDeleted(gf)
	verifyS3Deletion(gf)
}

func verifyObjectDeletionEvent(obj *registry.IntellectualObject) {
	params := url.Values{}
	params.Add("intellectual_object_id", strconv.FormatInt(obj.ID, 10))
	params.Add("event_type", constants.EventDeletion)
	params.Add("generic_file_id__is_null", "true")
	params.Add("sort", "date_time__desc")
	params.Add("page", "1")
	params.Add("per_page", "1")
	resp := ctx.Context.RegistryClient.PremisEventList(params)
	require.Nil(ctx.T, resp.Error)
	deletionEvent := resp.PremisEvent()
	require.NotNil(ctx.T, deletionEvent, obj.Identifier)

	assert.Equal(ctx.T, "APTrust preservation services", deletionEvent.Agent)
	assert.Equal(ctx.T, constants.EventDeletion, deletionEvent.EventType)
	assert.Equal(ctx.T, obj.ID, deletionEvent.IntellectualObjectID)
	assert.Equal(ctx.T, "Object deleted at the request of admin@test.edu. Institutional approver: admin@test.edu.", deletionEvent.OutcomeInformation)
	assert.NotEmpty(ctx.T, deletionEvent.DateTime)
	assert.NotEmpty(ctx.T, deletionEvent.CreatedAt)
	assert.NotEmpty(ctx.T, deletionEvent.UpdatedAt)
	assert.Empty(ctx.T, deletionEvent.GenericFileID)
}

func verifyFileDeletionEvent(gf *registry.GenericFile) {
	params := url.Values{}
	params.Add("event_type", constants.EventDeletion)
	params.Add("generic_file_id", strconv.FormatInt(gf.ID, 10))
	params.Add("sort", "date_time__desc")
	params.Add("page", "1")
	params.Add("per_page", "1")
	resp := ctx.Context.RegistryClient.PremisEventList(params)
	require.Nil(ctx.T, resp.Error)
	deletionEvent := resp.PremisEvent()
	require.NotNil(ctx.T, deletionEvent, gf.Identifier)

	assert.Equal(ctx.T, "APTrust preservation services", deletionEvent.Agent)
	assert.Equal(ctx.T, constants.EventDeletion, deletionEvent.EventType)
	assert.Equal(ctx.T, gf.IntellectualObjectID, deletionEvent.IntellectualObjectID)
	assert.Equal(ctx.T, "File deleted at the request of admin@test.edu. Institutional approver: admin@test.edu.", deletionEvent.OutcomeInformation)
	assert.NotEmpty(ctx.T, deletionEvent.DateTime)
	assert.NotEmpty(ctx.T, deletionEvent.CreatedAt)
	assert.NotEmpty(ctx.T, deletionEvent.UpdatedAt)
	assert.Equal(ctx.T, gf.ID, deletionEvent.GenericFileID)
}

func verifyStorageRecordsDeleted(gf *registry.GenericFile) {
	params := url.Values{}
	params.Add("generic_file_id", strconv.FormatInt(gf.ID, 10))
	params.Add("page", "1")
	params.Add("per_page", "20")
	resp := ctx.Context.RegistryClient.StorageRecordList(params)
	require.Nil(ctx.T, resp.Error)
	require.Empty(ctx.T, resp.StorageRecords())
	assert.Equal(ctx.T, 0, resp.Count)
}

func verifyS3Deletion(gf *registry.GenericFile) {
	for _, bucket := range ctx.Context.Config.PreservationBuckets {
		_, err := ctx.Context.S3StatObject(bucket.Provider, bucket.Bucket, gf.UUID)
		assert.NotNil(ctx.T, err)
		minioErr, ok := err.(minio.ErrorResponse)
		require.True(ctx.T, ok)
		assert.Equal(ctx.T, "NoSuchKey", minioErr.Code)
	}
}

func verifyFileDeletionWorkItem(gf *registry.GenericFile) {
	params := url.Values{}
	params.Add("generic_file_id", strconv.FormatInt(gf.ID, 10))
	params.Add("action", constants.ActionDelete)
	params.Add("page", "1")
	params.Add("per_page", "10")
	params.Add("sort", "date_processed__desc")
	resp := ctx.Context.RegistryClient.WorkItemList(params)
	require.Nil(ctx.T, resp.Error, gf.ID)
	assert.Equal(ctx.T, 1, resp.Count, gf.ID)
	item := resp.WorkItem()
	require.NotNil(ctx.T, item, gf.ID)
	assert.Equal(ctx.T, constants.StageResolve, item.Stage)
	assert.Equal(ctx.T, constants.StatusSuccess, item.Status)

	assert.Equal(ctx.T, constants.StageResolve, item.Stage)
	assert.Equal(ctx.T, constants.StatusSuccess, item.Status)
	assert.Equal(ctx.T, "Deletion completed at the request of admin@test.edu, approved by admin@test.edu.", item.Note)
	assert.Equal(ctx.T, "File deletion complete", item.Outcome)
}

func verifyObjectDeletionWorkItem(obj *registry.IntellectualObject) {
	params := url.Values{}
	params.Add("intellectual_object_id", strconv.FormatInt(obj.ID, 10))
	params.Add("generic_file_id__is_null", "true")
	params.Add("action", constants.ActionDelete)
	params.Add("page", "1")
	params.Add("per_page", "10")
	params.Add("sort", "date_processed__desc")
	resp := ctx.Context.RegistryClient.WorkItemList(params)
	require.Nil(ctx.T, resp.Error)
	assert.Equal(ctx.T, 1, resp.Count)
	item := resp.WorkItem()
	assert.Equal(ctx.T, constants.StageResolve, item.Stage)
	assert.Equal(ctx.T, constants.StatusSuccess, item.Status)

	assert.Equal(ctx.T, constants.StageResolve, item.Stage)
	assert.Equal(ctx.T, constants.StatusSuccess, item.Status)
	assert.Equal(ctx.T, "Deletion completed at the request of admin@test.edu, approved by admin@test.edu.", item.Note)
	assert.Equal(ctx.T, "Object deletion complete", item.Outcome)
}
