package ingest_test

import (
	//"bytes"
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	//"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

var key = "example.edu.tagsample_good.tar"
var testbag = testutil.PathToUnitTestBag(key)
var testbagMd5 = "f4323e5e631834c50d077fc3e03c2fed"
var testbagSize = int64(32256)
var s3files = []string{
	"aptrust-info.txt",
	"bag-info.txt",
	"bagit.txt",
	"manifest-md5.txt",
	"manifest-sha256.txt",
	"tagmanifest-md5.txt",
	"tagmanifest-sha256.txt",
}

// Make sure the bag we want to work on is in S3 before we
// start our tests.
func setupS3(t *testing.T, context *common.Context) {
	clearS3Files(t, context)
	putBagInS3(t, context)
}

// Get rid of text files that may be lingering in our local
// in-memory S3 server from the previous test.
func clearS3Files(t *testing.T, context *common.Context) {
	for _, filename := range s3files {
		key := fmt.Sprintf("9999/%s", filename)
		_ = context.S3Clients[constants.S3ClientAWS].RemoveObject(
			constants.TestBucketReceiving,
			key)
		//require.Nil(t, err)
	}
}

// Copy testbag to local in-memory S3 service.
func putBagInS3(t *testing.T, context *common.Context) {
	context.S3Clients[constants.S3ClientAWS].TraceOn(os.Stderr)
	bytesWritten, err := context.S3Clients[constants.S3ClientAWS].FPutObject(
		constants.TestBucketReceiving,
		key,
		testbag,
		minio.PutObjectOptions{})
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	require.Nil(t, err, msg)
	assert.True(t, (bytesWritten >= testbagSize))
}

// Returns an IngestObject that describes the tarred bag waiting
// in our receiving bucket.
func getIngestObject() *service.IngestObject {
	return service.NewIngestObject(
		constants.TestBucketReceiving, // bucket
		filepath.Base(testbag),        // key
		testbagMd5,                    // eTag
		"example.edu",                 // institution
		testbagSize,                   // size
	)
}

func TestNewMetadataGatherer(t *testing.T) {
	context := common.NewContext()
	g := ingest.NewMetadataGatherer(context)
	require.NotNil(t, g)
	assert.Equal(t, context, g.Context)
}

func TestGetS3Object(t *testing.T) {
	context := common.NewContext()
	setupS3(t, context)
	g := ingest.NewMetadataGatherer(context)
	ingestObject := getIngestObject()

	minioObj, err := g.GetS3Object(ingestObject)
	require.NotNil(t, minioObj)
	defer minioObj.Close()
	require.Nil(t, err)

	fmt.Println(minioObj)
	localFile, _ := os.Create("/Users/diamond/Desktop/bag.tar")
	io.Copy(localFile, minioObj)

	fmt.Println(minioObj.Stat())
	require.Nil(t, err)
	//assert.True(t, (bytesRead > testbagSize))
}

func TestScanBag(t *testing.T) {
	context := common.NewContext()
	setupS3(t, context)
	g := ingest.NewMetadataGatherer(context)
	ingestObject := getIngestObject()

	err := g.ScanBag(9999, ingestObject)
	require.Nil(t, err)
}

func testRedisRecords(t *testing.T) {
	// Make sure all expected records are in local redis server.
}

func testS3Files(t *testing.T) {
	// Make sure all expected files were copied to local S3 server.
}
