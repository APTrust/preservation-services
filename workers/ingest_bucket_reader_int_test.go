// +build integration

package workers_test

import (
	"io/ioutil"
	"net/url"
	"path"
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/APTrust/preservation-services/workers"
	"github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngestBucketReader_Run(t *testing.T) {
	context := common.NewContext()
	putBagsInTestReceiving(t, context)

	preTestCount := int(getStatsCount(t, context))

	// Bucket reader should add four items from int_test_bags/original
	// to the queue.
	reader := workers.NewIngestBucketReader()
	reader.Run()
	assertStatsCount(t, context, preTestCount+4)

	// Make sure the records are in Pharos as well.
	assertWorkItems(t, context)
}

func assertStatsCount(t *testing.T, context *common.Context, expected int) {
	count := getStatsCount(t, context)
	assert.EqualValues(t, expected, count)
}

func getStatsCount(t *testing.T, context *common.Context) uint64 {
	stats, procErr := context.NSQClient.GetStats()
	require.Nil(t, procErr)
	topic := stats.GetTopic(constants.IngestPreFetch)
	require.NotNil(t, topic)
	return topic.MessageCount
}

func assertWorkItems(t *testing.T, context *common.Context) {
	files := getFileList(t)
	for _, file := range files {
		v := url.Values{}
		v.Set("name", path.Base(file))
		resp := context.PharosClient.WorkItemList(v)
		require.Nil(t, resp.Error)
		assert.Equal(t, 1, len(resp.WorkItems()))
	}
}

func putBagsInTestReceiving(t *testing.T, context *common.Context) {
	files := getFileList(t)
	for _, file := range files {
		_, err := context.S3Clients[constants.StorageProviderAWS].FPutObject(
			constants.TestEduReceivingBucket,
			path.Base(file),
			file,
			minio.PutObjectOptions{})
		require.Nil(t, err, file)
	}
}

func getFileList(t *testing.T) []string {
	filenames := make([]string, 0)
	dir := path.Join(testutil.PathToTestData(), "int_test_bags", "original")
	files, err := ioutil.ReadDir(dir)
	require.Nil(t, err)
	for _, file := range files {
		filenames = append(filenames, path.Join(dir, file.Name()))
	}
	return filenames
}