package util_test

import (
	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestFileExists(t *testing.T) {
	f := testutil.PathToUnitTestBag("example.edu.sample_good.tar")
	assert.True(t, util.FileExists(f))
	assert.True(t, util.FileExists(util.ProjectRoot()))
	assert.False(t, util.FileExists("NonExistentFile.xyz"))
}

func TestExpandTilde(t *testing.T) {
	expanded, err := util.ExpandTilde("~/tmp")
	assert.Nil(t, err)
	assert.True(t, len(expanded) > 6)
	assert.True(t, strings.HasSuffix(expanded, "tmp"))

	expanded, err = util.ExpandTilde("/nothing/to/expand")
	assert.Nil(t, err)
	assert.Equal(t, "/nothing/to/expand", expanded)
}

func TestLooksSafeToDelete(t *testing.T) {
	assert.True(t, util.LooksSafeToDelete("/mnt/apt/data/some_dir", 15, 3))
	assert.False(t, util.LooksSafeToDelete("/usr/local", 12, 3))
}
