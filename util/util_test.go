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

func TestGetAlgFromManifestName(t *testing.T) {
	names := map[string]string{
		"manifest-md5.txt":       "md5",
		"tagmanifest-sha256.txt": "sha256",
		"manifest-sha512.txt":    "sha512",
	}
	for filename, algorithm := range names {
		alg, err := util.GetAlgFromManifestName(filename)
		assert.Nil(t, err)
		assert.Equal(t, algorithm, alg)
	}
	_, err := util.GetAlgFromManifestName("bad-file-name.txt")
	assert.NotNil(t, err)
}
