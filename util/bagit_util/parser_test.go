package bagit_util_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/bagit_util"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"testing"
)

var tagLabels = []string{
	"Source-Organization",
	"Bagging-Date",
	"Bag-Count",
	"Bag-Group-Identifier",
	"Internal-Sender-Description",
	"Internal-Sender-Identifier",
}
var tagValues = []string{
	"virginia.edu",
	"2014-04-14T11:55:26.17-0400",
	"1 of 1",
	"Charley Horse",
	"so much depends upon a red wheel barrow glazed with rain water beside the white chickens",
	"uva-internal-id-0001",
}

var digests = []string{
	"248fac506a5c46b3c760312b99827b6fb5df4698d6cf9a9cdc4c54746728ab99",
	"8e3634d207017f3cfc8c97545b758c9bcd8a7f772448d60e196663ac4b62456a",
	"299e1c23e398ec6699976cae63ef08167201500fa64bcf18062111e0c81d6a13",
	"cf9cbce80062932e10ee9cd70ec05ebc24019deddfea4e54b8788decd28b4bc7",
}

var paths = []string{
	"data/datastream-DC",
	"data/datastream-MARC",
	"data/datastream-RELS-EXT",
	"data/datastream-descMetadata",
}

func TestParseTagFile(t *testing.T) {
	tagfile := path.Join(testutil.PathToTestData(), "files", "bag-info.txt")
	file, err := os.Open(tagfile)
	require.Nil(t, err)
	defer file.Close()
	tags, err := bagit_util.ParseTagFile(file, "bag-info.txt")
	require.Nil(t, err)
	assert.Equal(t, len(tagLabels), len(tags))
	for i, tag := range tags {
		assert.Equal(t, "bag-info.txt", tag.SourceFile)
		assert.Equal(t, tagLabels[i], tag.Label)
		assert.Equal(t, tagValues[i], tag.Value)
	}
}

func TestParseManifest(t *testing.T) {
	manifest := path.Join(testutil.PathToTestData(), "files", "manifest-sha256.txt")
	file, err := os.Open(manifest)
	require.Nil(t, err)
	defer file.Close()

	alg, err := util.GetAlgFromManifestName(manifest)
	require.Nil(t, err)
	assert.Equal(t, constants.AlgSha256, alg)

	checksums, err := bagit_util.ParseManifest(file, alg)
	require.Nil(t, err)

	assert.Equal(t, len(digests), len(checksums))
	for i, cs := range checksums {
		assert.Equal(t, constants.AlgSha256, cs.Algorithm)
		assert.Equal(t, digests[i], cs.Digest)
		assert.Equal(t, paths[i], cs.Path)
	}
}
