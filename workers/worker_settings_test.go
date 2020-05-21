package workers_test

import (
	"testing"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/workers"
	"github.com/stretchr/testify/assert"
)

func TestToJSON(t *testing.T) {
	settings := &workers.Settings{
		ChannelBufferSize:                         20,
		DeleteFromReceivingAfterFatalError:        false,
		DeleteFromReceivingAfterMaxFailedAttempts: false,
		MaxAttempts:                         3,
		NSQChannel:                          constants.IngestPreFetch + "_worker_chan",
		NSQTopic:                            constants.IngestPreFetch,
		NextQueueTopic:                      constants.IngestValidation,
		NextWorkItemStage:                   constants.StageValidate,
		NumberOfWorkers:                     2,
		PushToCleanupAfterMaxFailedAttempts: false,
		PushToCleanupOnFatalError:           false,
		RequeueTimeout:                      (1 * time.Minute),
		WorkItemSuccessNote:                 "Finished pre-fetch metadata gathering",
	}
	assert.Equal(t, expectedJSON, settings.ToJSON())
}

var expectedJSON = `{"ChannelBufferSize":20,"DeleteFromReceivingAfterFatalError":false,"DeleteFromReceivingAfterMaxFailedAttempts":false,"MaxAttempts":3,"NSQChannel":"ingest01_prefetch_worker_chan","NSQTopic":"ingest01_prefetch","NextQueueTopic":"ingest02_bag_validation","NextWorkItemStage":"Validate","NumberOfWorkers":2,"PushToCleanupAfterMaxFailedAttempts":false,"PushToCleanupOnFatalError":false,"RequeueTimeout":60000000000,"WorkItemSuccessNote":"Finished pre-fetch metadata gathering"}`
