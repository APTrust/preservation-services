package network_test

import (
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/network"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

var nonFatalErr = service.NewProcessingError(999, "test.edu/obj", "Non-fatal error", false)
var fatalErr = service.NewProcessingError(333, "test.edu/obj", "Fatal error", true)

func getRedisClient() *network.RedisClient {
	config := common.NewConfig()
	return network.NewRedisClient(
		config.RedisURL,
		config.RedisPassword,
		config.RedisDefaultDB,
	)
}

func TestNewRedisClient(t *testing.T) {
	client := getRedisClient()
	assert.NotNil(t, client)
}

func TestRedisPing(t *testing.T) {
	client := getRedisClient()
	response, err := client.Ping()
	assert.Nil(t, err)
	assert.Equal(t, "PONG", response)
}

func TestIngestObjectSaveAndGet(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)
	obj := service.NewIngestObject("bucket", "key", "etag", "test.edu", 9855, int64(555))
	err := client.IngestObjectSave(9999, obj)
	assert.Nil(t, err)

	retrievedObj, err := client.IngestObjectGet(9999, obj.Identifier())
	assert.Nil(t, err)
	assert.NotNil(t, retrievedObj)
	assert.Equal(t, obj.ETag, retrievedObj.ETag)
	assert.Equal(t, obj.S3Bucket, retrievedObj.S3Bucket)
	assert.Equal(t, obj.S3Key, retrievedObj.S3Key)
}

func TestIngestObjectDelete(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)
	obj := service.NewIngestObject("bucket", "key", "etag", "test.edu", 9855, int64(555))
	err := client.IngestObjectSave(9999, obj)
	assert.Nil(t, err)

	err = client.IngestObjectDelete(9999, obj.Identifier())
	assert.Nil(t, err)
}

func TestIngestFileSaveAndGet(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)
	f := service.NewIngestFile("test.edu/bag1", "data/images.photo.jpg")
	err := client.IngestFileSave(9999, f)
	assert.Nil(t, err)

	retrievedFile, err := client.IngestFileGet(9999, f.Identifier())
	assert.Nil(t, err)
	assert.NotNil(t, retrievedFile)
	assert.Equal(t, f.ObjectIdentifier, retrievedFile.ObjectIdentifier)
	assert.Equal(t, f.PathInBag, retrievedFile.PathInBag)
}

func TestIngestFileDelete(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)
	f := service.NewIngestFile("test.edu/bag1", "data/images.photo.jpg")
	err := client.IngestFileSave(9999, f)
	assert.Nil(t, err)

	err = client.IngestFileDelete(9999, f.Identifier())
	assert.Nil(t, err)
}

func TestRestorationObjectSaveAndGet(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)
	obj := testutil.GetRestorationObject()
	err := client.RestorationObjectSave(9999, obj)
	assert.Nil(t, err)

	retrievedObj, err := client.RestorationObjectGet(9999, obj.Identifier)
	assert.Nil(t, err)
	assert.NotNil(t, retrievedObj)
	assert.Equal(t, obj.Identifier, retrievedObj.Identifier)
	assert.Equal(t, obj.URL, retrievedObj.URL)
}

func TestRestorationObjectDelete(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)
	obj := testutil.GetRestorationObject()
	err := client.RestorationObjectSave(9999, obj)
	assert.Nil(t, err)

	err = client.IngestObjectDelete(9999, obj.Identifier)
	assert.Nil(t, err)
}

func TestWorkItemDelete(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)

	// Save an object...
	obj := service.NewIngestObject("bucket", "bag1.tar",
		"etag", "test.edu", 9855, int64(555))
	err := client.IngestObjectSave(9999, obj)
	assert.Nil(t, err)

	// Make sure it's there.
	objFromRedis, err := client.IngestObjectGet(9999, "test.edu/bag1")
	assert.NotNil(t, objFromRedis)
	assert.Nil(t, err)

	// and some files...
	files := []string{
		"data/images.photo01.jpg",
		"data/images.photo02.jpg",
		"data/images.photo03.jpg",
		"data/images.photo04.jpg",
	}
	for _, filename := range files {
		f := service.NewIngestFile("test.edu/bag1", filename)
		err = client.IngestFileSave(9999, f)
		assert.Nil(t, err)
	}

	// Make sure files are there.
	for _, filename := range files {
		fileIdentifier := fmt.Sprintf("test.edu/bag1/%s", filename)
		f, err := client.IngestFileGet(9999, fileIdentifier)
		assert.NotNil(t, f)
		assert.Nil(t, err)
	}

	// Call delete to get rid of all records associated with this
	// WorkItem.
	itemsDeleted, err := client.WorkItemDelete(9999)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, itemsDeleted)

	// Make sure the IngestObject record was actually deleted.
	objFromRedis, err = client.IngestObjectGet(9999, "test.edu/bag1")
	assert.Nil(t, objFromRedis)
	assert.Equal(t, "IngestObjectGet (9999, test.edu/bag1): redis: nil", err.Error())

	// Make sure all of the IngestFile objects were deleted.
	for _, filename := range files {
		fileIdentifier := fmt.Sprintf("test.edu/bag1/%s", filename)
		f, err := client.IngestFileGet(9999, fileIdentifier)
		assert.Nil(t, f)
		assert.NotNil(t, err)
	}
}

func TestGetBatchOfFileKeys(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)

	testGetBatch(t, client, 10, 3)
	testGetBatch(t, client, 100, 12)
}

func testGetBatch(t *testing.T, client *network.RedisClient, totalItems, batchSize int) {
	for i := 0; i < totalItems; i++ {
		f := service.NewIngestFile("test.edu/bag1",
			fmt.Sprintf("file_%d.jpg", i))
		err := client.IngestFileSave(5555, f)
		assert.Nil(t, err)
	}

	defer client.WorkItemDelete(5555)

	//time.Sleep(250 * time.Millisecond)

	nextOffset := uint64(0)
	count := 0
	var fileMap map[string]*service.IngestFile
	var err error
	for {
		fileMap, nextOffset, err = client.GetBatchOfFileKeys(
			5555, nextOffset, int64(batchSize))
		require.Nil(t, err)
		count += len(fileMap)
		if nextOffset == 0 {
			break
		}
	}
	assert.Equal(t, totalItems, count)
}

func TestIngestFileApply(t *testing.T) {
	workItemId := 7654
	client := getRedisClient()
	require.NotNil(t, client)

	// Create 20 ingest file records in Redis.
	// They all have format "text/xml"
	for i := 0; i < 20; i++ {
		f := service.NewIngestFile("test.edu/bag1",
			fmt.Sprintf("file_%d.jpg", i))
		f.FileFormat = "text/xml"
		err := client.IngestFileSave(workItemId, f)
		assert.Nil(t, err)
	}

	// Create a function that changes IngestFile.FileFormat
	// to "text/plain"
	fn := func(ingestFile *service.IngestFile) (errors []*service.ProcessingError) {
		ingestFile.FileFormat = "text/plain"
		return errors
	}

	// Apply the function above to all IngestFile records
	// with our workItemId.
	options := service.IngestFileApplyOptions{
		MaxErrors:   1,
		MaxRetries:  1,
		RetryMs:     0,
		SaveChanges: true,
		WorkItemID:  workItemId,
	}
	count, errors := client.IngestFilesApply(fn, options)
	require.Empty(t, errors, errors)
	assert.Equal(t, 20, count)

	// Make sure all records were actually updated.
	fileMap, _, err := client.GetBatchOfFileKeys(workItemId, 0, int64(50))
	require.Nil(t, err)
	for _, ingestFile := range fileMap {
		assert.Equal(t, "text/plain", ingestFile.FileFormat)
	}
}

func TestIngestFileApply_WithError(t *testing.T) {
	workItemId := 988762
	client := getRedisClient()
	require.NotNil(t, client)

	// Create 20 ingest file records in Redis.
	// They all have format "text/xml"
	for i := 0; i < 20; i++ {
		f := service.NewIngestFile("test.edu/bag1",
			fmt.Sprintf("file_%d.jpg", i))
		f.FileFormat = "text/xml"
		err := client.IngestFileSave(workItemId, f)
		assert.Nil(t, err)
	}

	// Create a function that throws an error when it
	// finds file_12.jpg
	fn := func(ingestFile *service.IngestFile) (errors []*service.ProcessingError) {
		if strings.Contains(ingestFile.Identifier(), "12") {
			errors = append(errors, service.NewProcessingError(
				workItemId,
				ingestFile.Identifier(),
				"Found file 12",
				false,
			))
		}
		return errors
	}

	// Apply the function above to all IngestFile records
	// with our workItemId. It should throw an error on 12.
	options := service.IngestFileApplyOptions{
		MaxErrors:   1,
		MaxRetries:  1,
		RetryMs:     0,
		SaveChanges: false,
		WorkItemID:  workItemId,
	}
	count, errors := client.IngestFilesApply(fn, options)
	require.Equal(t, 1, len(errors))
	errMsg := errors[0].Error()
	assert.True(t, strings.Contains(errMsg, "Found file 12"), errMsg)

	// We should get back the number of files
	// on which the function was run successfully.
	// Because Redis loops through records in random
	// order, we can't really know what this number
	// will be.
	assert.True(t, count >= 0)
}

func TestWorkResultSaveAndGet(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)
	result := service.NewWorkResult(constants.IngestPreFetch)
	result.AddError(fatalErr)
	err := client.WorkResultSave(9999, result)
	assert.Nil(t, err)

	retrievedResult, err := client.WorkResultGet(9999, result.Operation)
	assert.Nil(t, err)
	assert.NotNil(t, retrievedResult)
	assert.Equal(t, constants.IngestPreFetch, retrievedResult.Operation)
	assert.Equal(t, 1, len(retrievedResult.Errors))
	assert.Equal(t, fatalErr, retrievedResult.Errors[0])
	assert.True(t, retrievedResult.HasFatalErrors())
}

func TestWorkResultSaveAndDelete(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)
	result := service.NewWorkResult(constants.IngestPreFetch)
	err := client.WorkResultSave(9999, result)
	assert.Nil(t, err)

	retrievedResult, err := client.WorkResultGet(9999, result.Operation)
	assert.Nil(t, err)
	assert.NotNil(t, retrievedResult)

	err = client.WorkResultDelete(9999, constants.IngestPreFetch)
	require.Nil(t, err)

	deletedResult, _ := client.WorkResultGet(9999, result.Operation)
	assert.Nil(t, deletedResult)
}

func TestKeys(t *testing.T) {
	client := getRedisClient()
	require.NotNil(t, client)
	result := service.NewWorkResult(constants.IngestPreFetch)
	err := client.WorkResultSave(654321, result)
	assert.Nil(t, err)

	keys, err := client.Keys("*")
	require.Nil(t, err)
	assert.True(t, len(keys) > 0)

	keys, err = client.Keys("654*")
	require.Nil(t, err)
	assert.Equal(t, 1, len(keys))
}
