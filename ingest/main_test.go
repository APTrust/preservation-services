package ingest_test

import (
	"fmt"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
)

var RedisTestServer *testutil.RedisServer
var S3TestServer *testutil.S3Server

func TestMain(m *testing.M) {
	startServers()
	exitCode := m.Run()
	stopServers()
	os.Exit(exitCode)
}

func startServers() {
	if RedisTestServer == nil {
		RedisTestServer = testutil.NewRedisServer()
	}
	if S3TestServer == nil {
		S3TestServer = testutil.NewS3Server()
	}
}

func stopServers() {
	if RedisTestServer != nil {
		RedisTestServer.Close()
		RedisTestServer = nil
	}
	if S3TestServer == nil {
		S3TestServer.Close()
		S3TestServer = nil
	}
}

// This creates a Minio S3 client that talks to our local S3 test server.
// It stores the client in context.S3Clients["LocalTest"], where our
// tests can access it.
//
// Use minio.NewWithRegion instead of minio.New. Minio's internal lookup
// can't find the region for localhost and puts 5 newline characters into
// the auth header, making it invalid.
func initS3TestClient(t *testing.T, context *common.Context) {
	if _, ok := context.S3Clients["LocalTest"]; !ok {
		url := strings.Replace(S3TestServer.URL, "http://", "", 1)
		fmt.Println(url)
		s3TestClient, err := minio.NewWithRegion(url, "test", "test", false, "local")
		require.Nil(t, err)
		require.NotNil(t, s3TestClient)
		context.S3Clients["LocalTest"] = s3TestClient
	}
}
