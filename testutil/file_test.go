package testutil_test

import (
	"github.com/APTrust/preservation-services/testutil"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"strings"
	"testing"
)

func TestProjectRoot(t *testing.T) {
	projectRoot := testutil.ProjectRoot()
	assert.True(t, len(projectRoot) > 2)
	assert.True(t, strings.Contains(projectRoot, string(os.PathSeparator)))
}

func TestPathToTestData(t *testing.T) {
	assert.True(t, strings.HasSuffix(testutil.PathToTestData(), "testdata"))
}

func TestPathToUnitTestBags(t *testing.T) {
	assert.True(t, strings.HasSuffix(testutil.PathToUnitTestBags(), "unit_test_bags"))
}

func TestPathToUnitTestBag(t *testing.T) {
	expectedSuffix := path.Join("unit_test_bags", "some-bag.tar")
	assert.True(t, strings.HasSuffix(testutil.PathToUnitTestBag("some-bag.tar"), expectedSuffix))
}
