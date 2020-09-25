// +build e2e

package e2e_test

import (
	//"fmt"
	"path"
	"testing"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/e2e"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/util"
	//"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type E2ECtx struct {
	Context *common.Context
	T       *testing.T
}

var ctx E2ECtx

func TestEndToEnd(t *testing.T) {
	context := common.NewContext()
	ctx = E2ECtx{
		Context: context,
		T:       t,
	}
	copyInitialBagsToReceivingBucket()
	waitForInitialIngestCompletion()
	copyUpdatedBagsToReceivingBucket()
	waitForReingestCompletion()
}

/* -------------------------------------------------------------------------
TODO: USE MINIO COPY instead of FileCopy so each bag gets a unique etag.
      Otherwise, the ingest_bucket_reader thinks we've already processed
      the updated bags (apt-001, etc) because the etag did not change.
------------------------------------------------------------------------- */

func copyInitialBagsToReceivingBucket() {
	minioReceivingDir := path.Join(ctx.Context.Config.BaseWorkingDir, "minio", "aptrust.receiving.test.test.edu")
	for _, tb := range e2e.TestBags {
		if tb.IsValidBag && !tb.IsUpdate {
			dest := path.Join(minioReceivingDir, tb.TarFileName())
			_, err := util.CopyFile(dest, tb.PathToBag)
			require.Nil(ctx.T, err)
			ctx.Context.Logger.Infof("Copied %s to receiving bucket", tb.TarFileName())
		}
	}
}

func copyUpdatedBagsToReceivingBucket() {
	minioReceivingDir := path.Join(ctx.Context.Config.BaseWorkingDir, "minio", "aptrust.receiving.test.test.edu")
	for _, tb := range e2e.TestBags {
		if tb.IsValidBag && tb.IsUpdate {
			dest := path.Join(minioReceivingDir, tb.TarFileName())
			_, err := util.CopyFile(dest, tb.PathToBag)
			require.Nil(ctx.T, err)
			ctx.Context.Logger.Infof("Copied updated version of  %s to receiving bucket", tb.TarFileName())
		}
	}
}

func waitForInitialIngestCompletion() {
	for {
		if initialIngestsComplete() {
			break
		}
		time.Sleep(10 * time.Second)
	}
}

func waitForReingestCompletion() {
	for {
		if reingestsComplete() {
			break
		}
		time.Sleep(10 * time.Second)
	}
}

func testBagCount(includeInvalid, includeReingest bool) uint64 {
	count := uint64(0)
	for _, tb := range e2e.TestBags {
		if (includeInvalid || tb.IsValidBag) && (includeReingest || !tb.IsUpdate) {
			count++
		}
	}
	return count
}

func initialIngestsComplete() bool {
	return ingestsComplete(testBagCount(false, false))
}

func reingestsComplete() bool {
	return ingestsComplete(testBagCount(false, true))
}

func ingestsComplete(count uint64) bool {
	require.True(ctx.T, count > 0)
	stats, err := ctx.Context.NSQClient.GetStats()
	require.Nil(ctx.T, err)
	channelName := constants.IngestCleanup + "_worker_chan"
	summary, err := stats.GetChannelSummary(constants.IngestCleanup, channelName)
	require.Nil(ctx.T, err)
	ctx.Context.Logger.Infof("In %s: %d in flight, %d finished. Want %d", channelName, summary.InFlightCount, summary.FinishCount, count)
	return summary.InFlightCount == 0 && summary.FinishCount == count
}
