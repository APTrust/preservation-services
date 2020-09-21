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
// FixityChecker does not inherit from the Base worker because fixity checks
// do not have associated WorkItems or Redis models. Much of the underlying
// code in workers.Base handles WorkItem and Redis housekeeping that is not
// required here. In fact, that code would fail, since there are no WorkItems
// or Redis records to work with.
type FixityChecker struct {
	Context           *common.Context
	ProcessChannel    chan *Task
	SuccessChannel    chan *Task
	ErrorChannel      chan *Task
	FatalErrorChannel chan *Task
	Settings          *Settings
	NSQConsumer       *nsq.Consumer
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
		Context:           common.NewContext(),
		Settings:          settings,
		ProcessChannel:    make(chan *Task, settings.ChannelBufferSize),
		SuccessChannel:    make(chan *Task, settings.ChannelBufferSize),
		ErrorChannel:      make(chan *Task, settings.ChannelBufferSize),
		FatalErrorChannel: make(chan *Task, settings.ChannelBufferSize),
	}

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

// Tell NSQ we're listening
func (c *FixityChecker) RegisterAsNsqConsumer() error {
	config := nsq.NewConfig()
	config.Set("heartbeat_interval", "10s")
	config.Set("max_in_flight", c.Settings.ChannelBufferSize)
	consumer, err := nsq.NewConsumer(c.Settings.NSQTopic, c.Settings.NSQChannel, config)
	if err != nil {
		return err
	}
	c.NSQConsumer = consumer
	c.NSQConsumer.AddHandler(c)
	c.NSQConsumer.ConnectToNSQLookupd(c.Context.Config.NsqLookupd)
	c.Context.Logger.Info("Registered as NSQ consumer")
	c.Context.Logger.Infof("Topic: %s, Channel: %s", c.Settings.NSQTopic, c.Settings.NSQChannel)
	c.Context.Logger.Infof("Workers: %d", c.Settings.NumberOfWorkers)
	c.Context.Logger.Infof("Channel Buffer Size: %d", c.Settings.ChannelBufferSize)
	c.Context.Logger.Infof("Max Attempts: %d", c.Settings.MaxAttempts)
	return nil
}

// This method omits a lot of WorkItem housekeeping that the other workers
// need to do.
func (c *FixityChecker) HandleMessage(message *nsq.Message) error {
	gfIdentifier := strings.TrimSpace(string(message.Body))
	task, err := c.GetTaskObject(message, gfIdentifier)
	if err != nil {
		c.Context.Logger.Errorf("Could not get Task for WorkItem %s: %v", gfIdentifier, err)
		return err
	}
	c.Context.Logger.Infof("Starting attempt %d for %s", message.Attempts, gfIdentifier)
	c.ProcessChannel <- task
	return nil
}

// ProcessItem calls task.Processor.Run() and then routes the
// task to the SuccessChannel, the ErrorChannel, or the
// FatalErrorChannel, depending on the outcome.
func (c *FixityChecker) ProcessItem() {
	for task := range c.ProcessChannel {
		task.NSQStart()
		c.Context.Logger.Infof("GenericFile %s is in ProcessChannel", task.WorkItem.GenericFileIdentifier)
		task.WorkResult.Start()
		count, errors := task.Processor.Run()
		task.WorkResult.Errors = errors
		task.WorkResult.Finish()

		// TODO: Test that items are going into the right channel here...
		c.Context.Logger.Infof("GenericFile %s: count %d, errors %d", task.WorkItem.GenericFileIdentifier, count, len(errors))

		if task.WorkResult.HasFatalErrors() {
			c.FatalErrorChannel <- task
		} else if task.WorkResult.HasErrors() {
			c.ErrorChannel <- task
		} else {
			c.SuccessChannel <- task
		}
	}
}

func (c *FixityChecker) ProcessSuccessChannel() {
	for task := range c.SuccessChannel {
		c.Context.Logger.Infof("File %s: fixity matched", task.WorkItem.GenericFileIdentifier)
		task.NSQFinish()
	}
}

func (c *FixityChecker) ProcessErrorChannel() {
	for task := range c.ErrorChannel {
		shouldRequeue := int(task.NSQMessage.Attempts) < c.Settings.MaxAttempts
		c.Context.Logger.Warningf("File %s is in error channel", task.WorkItem.GenericFileIdentifier)
		c.Context.Logger.Warningf("Non-fatal errors for file %s: %s", task.WorkItem.GenericFileIdentifier, task.WorkResult.NonFatalErrorMessage())
		if shouldRequeue {
			c.Context.Logger.Infof("Requeueing %s", task.WorkItem.GenericFileIdentifier)
			task.NSQRequeue(c.Settings.RequeueTimeout)
		} else {
			c.Context.Logger.Infof("Not requeueing %s: max attempts exceeded", task.WorkItem.GenericFileIdentifier)
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

func (c *FixityChecker) GetTaskObject(message *nsq.Message, gfIdentifier string) (*Task, error) {
	fixityChecker := fixity.NewChecker(c.Context, gfIdentifier)
	workItem := &registry.WorkItem{
		ID:                    -1,
		GenericFileIdentifier: gfIdentifier,
	}
	workResult := service.NewWorkResult(constants.ActionFixityCheck)
	workResult.Attempt = int(message.Attempts)
	task := &Task{
		Processor:  fixityChecker,
		NSQMessage: message,
		WorkItem:   workItem,
		WorkResult: workResult,
	}
	return task, nil
}
