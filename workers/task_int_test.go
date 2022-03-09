//go:build integration
// +build integration

package workers_test

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/workers"
	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var consumer *nsq.Consumer
var tester *TaskTester
var wg sync.WaitGroup
var completedTestCount int

type TaskTester struct {
	T *testing.T
}

// Note that the tests are actually done in here...
func (tester *TaskTester) HandleMessage(message *nsq.Message) error {
	workItemId, _ := strconv.Atoi(string(message.Body))
	task := &workers.Task{
		NSQMessage: message,
	}
	task.NSQStart()
	if workItemId == 1111 {
		assert.True(tester.T, task.NSQMessage.IsAutoResponseDisabled())
		assert.True(tester.T, task.StartCalled())
		assert.False(tester.T, task.TickerStopped())
		wg.Done()
		completedTestCount++
	} else if workItemId == 2222 {
		task.NSQRequeue(50 * time.Minute)
		assert.True(tester.T, task.TickerStopped())
		wg.Done()
		completedTestCount++
	} else if workItemId == 3333 {
		task.NSQFinish()
		assert.True(tester.T, task.TickerStopped())
		wg.Done()
		completedTestCount++
	}
	return nil
}

func initConsumerAndTester(t *testing.T, context *common.Context) {
	var err error
	nsqConfig := nsq.NewConfig()
	nsqConfig.Set("max_in_flight", 20)
	nsqConfig.Set("max_attempts", 1) // important, or wg counter goes negative
	nsqConfig.Set("heartbeat_interval", 1000)
	consumer, err = nsq.NewConsumer("ingest_task_topic", "ingest_task_channel", nsqConfig)
	require.Nil(t, err)
	tester = &TaskTester{
		T: t,
	}
	consumer.AddHandler(tester)
	consumer.ConnectToNSQLookupd(context.Config.NsqLookupd)
}

func initTest(t *testing.T, workItemId int64) {
	context := common.NewContext()
	err := context.NSQClient.Enqueue("ingest_task_topic", workItemId)
	require.Nil(t, err)
	if consumer == nil {
		initConsumerAndTester(t, context)
	}
}

// This is the only function called by the testing framework.
// It pushes messages into NSQ, which are then handled
// by HandleMessage above. HandleMessage runs the actual tests.
func TestTask(t *testing.T) {
	wg.Add(3)
	initTest(t, 1111) // test Task.NSQStart()
	initTest(t, 2222) // test Task.NSQRequeue()
	initTest(t, 3333) // test Task.NSQFinish()
	wg.Wait()
	assert.Equal(t, 3, completedTestCount)
}
