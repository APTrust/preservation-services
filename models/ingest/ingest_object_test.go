package ingest_test

import (
	"github.com/APTrust/preservation-services/models/ingest"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewIngestObject(t *testing.T) {
	obj := ingest.NewIngestObject("bucket", "test-bag.b001.of200.tar", "\"123456\"", "test.edu", int64(500))
	assert.Equal(t, "123456", obj.ETag)
	assert.Equal(t, "test.edu/test-bag", obj.Identifier)
	assert.Equal(t, "test.edu", obj.Institution)
	assert.NotNil(t, obj.Manifests)
	assert.NotNil(t, obj.ParsableTagFiles)
	assert.Equal(t, "bucket", obj.S3Bucket)
	assert.Equal(t, "test-bag.b001.of200.tar", obj.S3Key)
	assert.EqualValues(t, 500, obj.Size)
	assert.NotNil(t, obj.TagManifests)
	assert.NotNil(t, obj.TopLevelDirs)
}
