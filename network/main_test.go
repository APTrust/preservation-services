package network_test

import (
	"github.com/APTrust/preservation-services/util/testutil"
	"os"
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
