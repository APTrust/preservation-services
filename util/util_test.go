package util_test

import (
	"github.com/APTrust/preservation-services/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStringListContains(t *testing.T) {
	list := []string{"apple", "orange", "banana"}
	assert.True(t, util.StringListContains(list, "orange"))
	assert.False(t, util.StringListContains(list, "wedgie"))
	// Don't crash on nil list
	assert.False(t, util.StringListContains(nil, "mars"))
}

func TestStringListContainsAll(t *testing.T) {
	list1 := []string{"apple", "orange", "banana"}
	list2 := []string{"apple", "orange", "banana"}
	list3 := []string{"apple", "orange", "fig"}

	assert.True(t, util.StringListContainsAll(list1, list2))
	assert.False(t, util.StringListContainsAll(list1, list3))
}

func TestAlgorithmFromManifestName(t *testing.T) {
	names := map[string]string{
		"manifest-md5.txt":       "md5",
		"tagmanifest-sha256.txt": "sha256",
		"manifest-sha512.txt":    "sha512",
	}
	for filename, algorithm := range names {
		alg, err := util.AlgorithmFromManifestName(filename)
		assert.Nil(t, err)
		assert.Equal(t, algorithm, alg)
	}
	_, err := util.AlgorithmFromManifestName("bad-file-name.txt")
	assert.NotNil(t, err)
}

func TestLooksLikeManifest(t *testing.T) {
	assert.True(t, util.LooksLikeManifest("manifest-md5.txt"))
	assert.True(t, util.LooksLikeManifest("manifest-sha256.txt"))
	// No: is tag manifest
	assert.False(t, util.LooksLikeManifest("tagmanifest-md5.txt"))
	// No: is tag file
	assert.False(t, util.LooksLikeManifest("bag-info.txt"))
	// No: is payload file
	assert.False(t, util.LooksLikeManifest("data/manifest-sha256.txt"))
}

func TestLooksLikeTagManifest(t *testing.T) {
	assert.True(t, util.LooksLikeTagManifest("tagmanifest-md5.txt"))
	assert.True(t, util.LooksLikeTagManifest("tagmanifest-sha256.txt"))
	// No: is manifest
	assert.False(t, util.LooksLikeTagManifest("manifest-md5.txt"))
	// No: is tag file
	assert.False(t, util.LooksLikeTagManifest("bag-info.txt"))
	// No: is payload file
	assert.False(t, util.LooksLikeTagManifest("data/manifest-sha256.txt"))
}
