package testutil_test

import (
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"path"
	"strings"
	"testing"
)

func TestPathToTestData(t *testing.T) {
	assert.True(t, strings.HasSuffix(testutil.PathToTestData(), "testdata"))
}

func TestPathToUnitTestBags(t *testing.T) {
	assert.True(t, strings.HasSuffix(testutil.PathToUnitTestBags(), "unit_test_bags"))
}

func TestPathPharosFixture(t *testing.T) {
	p := testutil.PathToPharosFixture("institutions.json")
	assert.True(t, strings.Contains(p, "testdata"))
	assert.True(t, strings.Contains(p, "pharos"))
	assert.True(t, strings.HasSuffix(p, "institutions.json"))
}

func TestPathToUnitTestBag(t *testing.T) {
	expectedSuffix := path.Join("unit_test_bags", "some-bag.tar")
	assert.True(t, strings.HasSuffix(testutil.PathToUnitTestBag("some-bag.tar"), expectedSuffix))
}
