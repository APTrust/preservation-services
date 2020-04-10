// +build integration

package ingest_test

import (
	"fmt"
	"testing"

	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const verifierItemID = 8944

func TestNewPreservationVerifier(t *testing.T) {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	verifier := ingest.NewPreservationVerifier(context, verifierItemID, obj)
	require.NotNil(t, verifier)
	assert.Equal(t, context, verifier.Context)
	assert.Equal(t, obj, verifier.IngestObject)
	assert.Equal(t, verifierItemID, verifier.WorkItemID)
}

func TestVerifyAll(t *testing.T) {
	context := common.NewContext()
	verifier := prepareForPreservationVerify(t, pathToGoodBag, verifierItemID, context)

	testFn := func(ingestFile *service.IngestFile) (errors []*service.ProcessingError) {
		for _, record := range ingestFile.StorageRecords {
			identifier := fmt.Sprintf("%s - %s:%s", ingestFile.Identifier(), record.Provider, record.Bucket)
			assert.NotEmpty(t, record.ETag, identifier)
			assert.Equal(t, ingestFile.Size, record.Size, identifier)
			assert.False(t, record.VerifiedAt.IsZero(), identifier)
			assert.Empty(t, record.Error, identifier)
		}
		return errors
	}

	// Run the verifier and make sure there were no errors.
	// Only 8 files from the bag should be stored in
	// preservation (no manifests or bagit.txt).
	// Count should be 8 files * 2 storage records each = 16.
	count, errors := verifier.VerifyAll()
	assert.Equal(t, 16, count)
	assert.Empty(t, errors)

	// Now run the post test to ensure the verifier added
	// the correct info to our StorageRecords.
	options := service.IngestFileApplyOptions{
		MaxErrors:   1,
		MaxRetries:  1,
		RetryMs:     0,
		SaveChanges: false,
		WorkItemID:  verifier.WorkItemID,
	}
	_, errors = verifier.Context.RedisClient.IngestFilesApply(testFn, options)
	assert.Empty(t, errors, errors)
}
