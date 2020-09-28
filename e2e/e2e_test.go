// +build e2e

package e2e_test

import (
	"testing"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/e2e"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/minio/minio-go/v6"
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
	uploadInitialBags()
	waitForInitialIngestCompletion()
	uploadUpdatedBags()
	waitForReingestCompletion()
}

func uploadInitialBags() {
	bags := make([]*e2e.TestBag, 0)
	for _, tb := range e2e.TestBags {
		if tb.IsValidBag && !tb.IsUpdate {
			bags = append(bags, tb)
		}
	}
	pushBagsToReceiving(bags)
}

func uploadUpdatedBags() {
	bags := make([]*e2e.TestBag, 0)
	for _, tb := range e2e.TestBags {
		if tb.IsValidBag && tb.IsUpdate {
			bags = append(bags, tb)
		}
	}
	pushBagsToReceiving(bags)
}

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
