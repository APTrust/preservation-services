package util_test

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/APTrust/preservation-services/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var tempDir, _ = ioutil.TempDir("", "prod-serv-test")
var tempFile = path.Join(tempDir, "test-pid-file.txt")

func TestIsRunningInOtherProcess(t *testing.T) {
	defer os.Remove(tempFile)

	// False, because there is no pid file
	assert.False(t, util.IsRunningInOtherProcess(tempFile))

	// True, because pid doesn't match
	ioutil.WriteFile(tempFile, []byte("0"), 0664)
	assert.False(t, util.IsRunningInOtherProcess(tempFile))

	// False, because pid in file matches our pid
	os.Remove(tempFile)
	util.WritePidFile(tempFile)
	assert.False(t, util.IsRunningInOtherProcess(tempFile))
}

func TestReadPidFile(t *testing.T) {
	defer os.Remove(tempFile)
	ioutil.WriteFile(tempFile, []byte("9499"), 0664)
	assert.Equal(t, 9499, util.ReadPidFile(tempFile))
}

func TestWritePidFile(t *testing.T) {
	defer os.Remove(tempFile)
	util.WritePidFile(tempFile)
	assert.Equal(t, os.Getpid(), util.ReadPidFile(tempFile))
}

func TestDeletePidFile(t *testing.T) {
	defer os.Remove(tempFile)
	util.WritePidFile(tempFile)
	assert.True(t, util.FileExists(tempFile))
	util.DeletePidFile(tempFile)
	assert.False(t, util.FileExists(tempFile))
}

func TestAgeOfPidFile(t *testing.T) {
	defer os.Remove(tempFile)
	util.WritePidFile(tempFile)
	time.Sleep(400 * time.Millisecond)
	expected, _ := time.ParseDuration("400ms")
	actual, err := util.AgeOfPidFile(tempFile)
	require.Nil(t, err)
	// Duration is in nanoseconds
	halfASecond := float64(500000000)
	assert.InDelta(t, expected, actual, halfASecond)
}

func TestProcessIsRunning(t *testing.T) {
	assert.False(t, util.ProcessIsRunning(-999))
	assert.True(t, util.ProcessIsRunning(os.Getpid()))
}
