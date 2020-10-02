// +build e2e

package e2e_test

import (
	"testing"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/e2e"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file contains helpers to do some setup and
// housekeeping but don't perform any actual tests.

// Set up a context for testing.
func initTestContext(t *testing.T) {
	objects, err := e2e.LoadObjectJSON()
	require.Nil(t, err)

	files, err := e2e.LoadGenericFileJSON()
	require.Nil(t, err)

	context := common.NewContext()
	ctx = E2ECtx{
		Context:         context,
		T:               t,
		ExpectedObjects: objects,
		ExpectedFiles:   files,
	}
	ctx.TestInstitution = getInstitution("test.edu")
}

// Push some bags into the receiving bucket using Minio.
// We do this instead of a simple filesystem copy because
// Pharos WorkItems use ETags to distinguish between versions
// of a bag. Minio creates ETags, file copying doesn't.
func pushBagsToReceiving(testbags []*e2e.TestBag) {
	client := ctx.Context.S3Clients[constants.StorageProviderAWS]
	for _, tb := range testbags {
		_, err := client.FPutObject(
			"aptrust.receiving.test.test.edu",
			tb.TarFileName(),
			tb.PathToBag,
			minio.PutObjectOptions{},
		)
		require.Nil(ctx.T, err)
		ctx.Context.Logger.Infof("Copied %s to receiving bucket", tb.PathToBag)
	}
}

// Check NSQ every 10 seconds to see whether all initial ingests
// are complete.
func waitForInitialIngestCompletion() {
	for {
		if initialIngestsComplete() {
			break
		}
		time.Sleep(10 * time.Second)
	}
}

// Check NSQ every 10 seconds to see whether all reingests
// are complete.
func waitForReingestCompletion() {
	for {
		if reingestsComplete() {
			break
		}
		time.Sleep(10 * time.Second)
	}
}

// This returns the number of bags expected to be ingested
// or reingested. The reingest count includes all ingests
// plus reingests.
func testBagCount(ingestType string) int64 {
	count := int64(0)
	for _, tb := range e2e.TestBags {
		if (ingestType == "ingest" && !tb.IsUpdate) || (ingestType == "reingest" && tb.IsUpdate) {
			count++
		}
	}
	return count
}

// Returns the number of expected ingests, based on test bags defined
// in TestBags (in expected.go)
func expectedIngestCount() int64 {
	return testBagCount("ingest")
}

// Returns the number of expected reingests, based on test bags defined
// in TestBags (in expected.go)
func expectedReingestCount() int64 {
	return testBagCount("reingest")
}

// Returns true if the initial version of our test bags have
// been ingested.
func initialIngestsComplete() bool {
	return ingestsComplete("e2e_ingest_post_test", expectedIngestCount())
}

// Returns true if the updated versions of our test bags have
// been ingested.
func reingestsComplete() bool {
	return ingestsComplete("e2e_reingest_post_test", expectedReingestCount())
}

// This queries NSQ to find the number of finished items in a channel.
func ingestsComplete(topicName string, desiredCount int64) bool {
	require.True(ctx.T, desiredCount > 0)
	stats, err := ctx.Context.NSQClient.GetStats()
	require.Nil(ctx.T, err)
	allComplete := false
	topicStats := stats.GetTopic(topicName)
	if topicStats == nil {
		// Topic won't exist until the first ingest/reingest is complete.
		ctx.Context.Logger.Infof("Topic %s hasn't been created yet", topicName)
	} else {
		ctx.Context.Logger.Infof("Topic %s has depth %d. Want %d", topicName, topicStats.Depth, desiredCount)
		allComplete = (topicStats.Depth == desiredCount)
	}
	return allComplete
}

// Returns an institution record from Pharos. Our "test.edu" institution
// will have a different ID each time we test, so we have to look it up.
func getInstitution(identifier string) *registry.Institution {
	resp := ctx.Context.PharosClient.InstitutionGet(identifier)
	assert.NotNil(ctx.T, resp)
	require.Nil(ctx.T, resp.Error)
	institution := resp.Institution()
	require.NotNil(ctx.T, institution)
	return institution
}
