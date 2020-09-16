package workers

import (
	"fmt"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/fixity"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/nsqio/go-nsq"
)

// FixityChecker is a worker that processes file and object deletion requests.
type FixityChecker struct {
	Base
}

// NewFixityChecker creates a new FixityChecker worker. Param context is a
// Context object with connections to S3, Redis, Pharos, and NSQ.
func NewFixityChecker(bufSize, numWorkers, maxAttempts int) *FixityChecker {
	settings := &Settings{
		ChannelBufferSize: bufSize,
		MaxAttempts:       maxAttempts,
		NSQChannel:        constants.TopicFixity + "_worker_chan",
		NSQTopic:          constants.TopicFixity,
		NextQueueTopic:    "",
		NextWorkItemStage: "",
		NumberOfWorkers:   numWorkers,
		RequeueTimeout:    (20 * time.Second),
	}
	checker := &FixityChecker{
		Base: Base{
			Context:           common.NewContext(),
			Settings:          settings,
			ItemsInProcess:    service.NewRingList(settings.ChannelBufferSize),
			ProcessChannel:    make(chan *Task, settings.ChannelBufferSize),
			SuccessChannel:    make(chan *Task, settings.ChannelBufferSize),
			ErrorChannel:      make(chan *Task, settings.ChannelBufferSize),
			FatalErrorChannel: make(chan *Task, settings.ChannelBufferSize),
		},
	}

	// Set these methods on base with our custom versions.
	// These methods are not defined at all in base. Failing
	// to set them will result in nil pointers and crashes.
	checker.Base.ShouldSkipThis = checker.ShouldSkipThis
	checker.Base.GetTaskObject = checker.GetTaskObject

	checker.Context.Logger.Info("FixityCheck worker started with the following settings:")
	checker.Context.Logger.Info(settings.ToJSON())
	checker.Context.Logger.Info("Config settings (omitting sensitive credentials):")
	checker.Context.Logger.Info(checker.Context.Config.ToJSON())

	// Spin up the go routines that will act as workers
	for i := 0; i < settings.NumberOfWorkers; i++ {
		checker.Context.Logger.Infof("Starting worker #%d", i+1)
		go checker.ProcessItem()
	}
	go checker.ProcessErrorChannel()
	go checker.ProcessFatalErrorChannel()
	go checker.ProcessSuccessChannel()

	err := checker.RegisterAsNsqConsumer()
	if err != nil {
		panic(fmt.Sprintf("Cannot register NSQ consumer: %v", err))
	}

	return checker
}

// Overrides Base version of this method, because we're not really working
// with a WorkItem. Unlike other queues, where the message body is a WorkItem
// ID (int), in this queue, it's a GenericFile identifier (string).
//
// This method omits a lot of WorkItem housekeeping that the other workers
// need to do.
func (c *FixityChecker) HandleMessage(message *nsq.Message) error {
	gfIdentifier := strings.TrimSpace(string(message.Body))
	workItem := &registry.WorkItem{
		ID:                    -1,
		GenericFileIdentifier: gfIdentifier,
	}
	workResult := c.GetWorkResult(workItem.ID)
	task, err := c.GetTaskObject(message, workItem, workResult)
	if err != nil {
		c.Context.Logger.Errorf("Could not get Task for WorkItem %d (%s): %v", workItem.ID, workItem.GenericFileIdentifier, err)
		return err
	}
	c.Context.Logger.Infof("Starting %s", gfIdentifier)
	c.ProcessChannel <- task
	return nil
}

func (c *FixityChecker) ProcessSuccessChannel() {
	for task := range c.SuccessChannel {
		c.Context.Logger.Infof("File %s is in success channel", task.WorkItem.GenericFileIdentifier)
		task.NSQFinish()
	}
}

func (c *FixityChecker) ProcessErrorChannel() {
	for task := range c.ErrorChannel {
		shouldRequeue := true
		c.Context.Logger.Warningf("File %s is in error channel", task.WorkItem.GenericFileIdentifier)
		c.Context.Logger.Warningf("Non-fatal errors for WorkItem %s: %s", task.WorkItem.GenericFileIdentifier, task.WorkResult.NonFatalErrorMessage())
		if shouldRequeue {
			task.NSQRequeue(c.Settings.RequeueTimeout)
		} else {
			task.NSQFinish()
		}
	}
}

func (c *FixityChecker) ProcessFatalErrorChannel() {
	for task := range c.FatalErrorChannel {
		c.Context.Logger.Errorf("File %s is in fatal error channel", task.WorkItem.GenericFileIdentifier)
		c.Context.Logger.Errorf("Fatal errors for file %s: %s", task.WorkItem.GenericFileIdentifier, task.WorkResult.FatalErrorMessage())
		task.NSQFinish()
	}
}

func (c *FixityChecker) GetTaskObject(message *nsq.Message, workItem *registry.WorkItem, workResult *service.WorkResult) (*Task, error) {
	fixityChecker := fixity.NewChecker(c.Context, workItem.GenericFileIdentifier)
	task := &Task{
		Processor:  fixityChecker,
		NSQMessage: message,
		WorkItem:   workItem,
		WorkResult: workResult,
	}
	return task, nil
}

// ShouldSkipThis returns true if there are any reasons not process this
// WorkItem. This method always returns false. The only reason to skip
// a fixity check is if the item is in Glacier-only storage. The underlying
// FixityChecker will figure that out.
func (c *FixityChecker) ShouldSkipThis(workItem *registry.WorkItem) bool {
	return false
}
