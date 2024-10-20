package platform_test

import (
	"archive/tar"
	"github.com/APTrust/preservation-services/platform"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"testing"
)

func setupMimeTest(t *testing.T) string {
	tempfile, err := ioutil.TempFile("", "mime_test")
	require.Nil(t, err)
	_, err = io.WriteString(tempfile, "This is a text file.")
	require.Nil(t, err)
	tempfile.Close()
	return tempfile.Name()
}

func teardownMimeTest(pathToTempFile string) {
	os.Remove(pathToTempFile)
}

// GetOwnerAndGroup should fill in the Uid and Gid fields of
// the tar header on Posix systems. On windows, it won't fill in
// anything, but it should not cause any errors.
func TestGetOwnerAndGroup(t *testing.T) {
	pathToTempFile := setupMimeTest(t)
	defer teardownMimeTest(pathToTempFile)
	tempfile, err := os.Open(pathToTempFile)
	require.Nil(t, err)

	finfo, err := tempfile.Stat()
	require.Nil(t, err)

	tarHeader := &tar.Header{}
	platform.GetOwnerAndGroup(finfo, tarHeader)
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" ||
		runtime.GOOS == "unix" || runtime.GOOS == "bsd" {
		// We just wrote these files, so their uid and gid
		// should match ours.
		assert.EqualValues(t, os.Getuid(), tarHeader.Uid)
		assert.EqualValues(t, os.Getgid(), tarHeader.Gid)
	}
}

func TestGetMountPointFromPath(t *testing.T) {
	tempfile, err := ioutil.TempFile("", "platform_test")
	require.Nil(t, err)
	mountpoint, err := platform.GetMountPointFromPath(tempfile.Name())
	assert.Nil(t, err)
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" ||
		runtime.GOOS == "unix" || runtime.GOOS == "bsd" {
		assert.True(t, mountpoint == "/" || mountpoint == "/tmp")
	} else if runtime.GOOS == "windows" {
		assert.Equal(t, tempfile.Name(), mountpoint)
	}
}
