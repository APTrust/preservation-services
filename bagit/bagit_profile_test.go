package bagit_test

import (
	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"path"
	"testing"
)

// This also implicitly tests BagItProfileFromJson
func TestBagItProfileLoad(t *testing.T) {
	filename := path.Join(testutil.ProjectRoot(), "profiles", "aptrust-v2.2.json")
	profile, err := bagit.BagItProfileLoad(filename)
	assert.Nil(t, err)
	require.NotNil(t, profile)

	// Spot check
	assert.Equal(t, "support@aptrust.org", profile.BagItProfileInfo.ContactEmail)
	assert.Equal(t, 14, len(profile.Tags))
	assert.Equal(t, "BagIt-Version", profile.Tags[0].TagName)
	assert.Equal(t, "Storage-Option", profile.Tags[13].TagName)
	assert.Equal(t, 7, len(profile.Tags[13].Values))

	// Test with bad filename
	_, err = bagit.BagItProfileLoad("__file_does_not_exist__")
	assert.NotNil(t, err)

	// Test with non-JSON file. This is a tar file.
	filename = path.Join(testutil.PathToUnitTestBag("example.edu.tagsample_good.tar"))
	_, err = bagit.BagItProfileLoad(filename)
	assert.NotNil(t, err)
}
