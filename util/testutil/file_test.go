package testutil_test

import (
	"path"
	"strings"
	"testing"

	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPathToTestData(t *testing.T) {
	assert.True(t, strings.HasSuffix(testutil.PathToTestData(), "testdata"))
}

func TestPathToUnitTestBags(t *testing.T) {
	assert.True(t, strings.HasSuffix(testutil.PathToUnitTestBags(), "unit_test_bags"))
}

func TestPathToIntTestBags(t *testing.T) {
	assert.True(t, strings.HasSuffix(testutil.PathToIntTestBags(), "int_test_bags"))
}

func TestPathToE2EFile(t *testing.T) {
	assert.True(t, strings.HasSuffix(testutil.PathToE2EFile("test.json"), path.Join("e2e_results", "test.json")))
}

func TestPathToUnitTestBag(t *testing.T) {
	expectedSuffix := path.Join("unit_test_bags", "some-bag.tar")
	assert.True(t, strings.HasSuffix(testutil.PathToUnitTestBag("some-bag.tar"), expectedSuffix))
}

func TestPathPharosFixture(t *testing.T) {
	p := testutil.PathToPharosFixture("institutions.json")
	assert.True(t, strings.Contains(p, "testdata"))
	assert.True(t, strings.Contains(p, "pharos"))
	assert.True(t, strings.HasSuffix(p, "institutions.json"))
}

func TestReadPharosFixture(t *testing.T) {
	bytes, err := testutil.ReadPharosFixture("institutions.json")
	require.Nil(t, err)
	assert.True(t, len(bytes) > 100)
}

func TestReadE2EFile(t *testing.T) {
	bytes, err := testutil.ReadE2EFile("objects.json")
	require.Nil(t, err)
	assert.True(t, len(bytes) > 100)
}
