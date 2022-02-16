package workers

import (
	"net/url"
	"strconv"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util"
)

type QueueFixity struct {
	Context    *common.Context
	Identifier string
}

// NewQueueFixity creates a new worker to push files needing
// a fixity check into the NSQ.
//
// The optional param identifier is a GenericFile identifier.
// If provided, only that item will be queued. This is useful
// for manual testing and spot checks.
//
// This relies on these config settings:
//
// MaxDaysSinceFixityCheck specifies the number of days between
// fixity checks. Any file that hasn't been checked in this many
// days is eligible to be queued.
//
// QueueFixityInterval specifies how often this should check
// for new files to queue. In production, this is usually 60 mintes.
//
// MaxFixityItemsPerRun specifies the maximum number of items
// to queue per run. In production, this is usually 2500, though
// it could be set higher when we have the bandwidth and want to
// clear out backlogs.
func NewQueueFixity(identifier string) *QueueFixity {
	return &QueueFixity{
		Context:    common.NewContext(),
		Identifier: identifier,
	}
}

func (q *QueueFixity) logStartup() {
	q.Context.Logger.Info("Starting with config settings:")
	q.Context.Logger.Info(q.Context.Config.ToJSON())
	q.Context.Logger.Infof("Scan interval: %s",
		q.Context.Config.QueueFixityInterval.String())
}

func (q *QueueFixity) RunOnce() {
	q.logStartup()
	q.run()
}

func (q *QueueFixity) RunAsService() {
	q.logStartup()
	for {
		q.run()
		time.Sleep(q.Context.Config.QueueFixityInterval)
	}
}

// Run retrieves a list of GenericFiles needing fixity checks and
// adds the Identifier of each file to the NSQ fixity_check topic.
// It stops after queuing maxFiles.
func (q *QueueFixity) run() {
	if q.Identifier != "" {
		q.queueOne()
	} else {
		q.queueList()
	}
}

func (q *QueueFixity) queueList() {
	// Get to work. Note that we don't have to filter on storage option
	// because Pharos now knows to exclude Glacier-only files from
	// "not_checked_since" queries.
	perPage := util.Min(100, q.Context.Config.MaxFixityItemsPerRun)
	params := url.Values{}
	itemsAdded := 0
	params.Set("per_page", strconv.Itoa(perPage))
	params.Set("page", "1")
	params.Set("sort", "last_fixity_check") // takes advantage of SQL index

	hours := q.Context.Config.MaxDaysSinceFixityCheck * 24 * -1
	sinceWhen := time.Now().Add(time.Duration(hours) * time.Hour).UTC()
	params.Set("not_checked_since", sinceWhen.Format(time.RFC3339))

	q.Context.Logger.Infof("Queuing up to %d files not checked since %s to topic %s", q.Context.Config.MaxFixityItemsPerRun, sinceWhen.Format(time.RFC3339), constants.TopicFixity)

	for {
		resp := q.Context.RegistryClient.GenericFileList(params)
		q.Context.Logger.Infof("GET %s", resp.Request.URL)
		if resp.Error != nil {
			q.Context.Logger.Errorf("Error getting GenericFile list from Pharos: %s", resp.Error)
		}
		for _, gf := range resp.GenericFiles() {
			if q.addToNSQ(gf) {
				itemsAdded += 1
			}
		}
		if resp.HasNextPage() == false || itemsAdded >= q.Context.Config.MaxFixityItemsPerRun {
			break
		}
		params = resp.ParamsForNextPage()
	}
}

func (q *QueueFixity) queueOne() {
	resp := q.Context.RegistryClient.GenericFileByIdentifier(q.Identifier)
	if resp.Error != nil {
		q.Context.Logger.Errorf("Error getting GenericFile list from Pharos: %s", resp.Error)
		return
	}
	q.addToNSQ(resp.GenericFile())
}

func (q *QueueFixity) addToNSQ(gf *registry.GenericFile) bool {
	err := q.Context.NSQClient.EnqueueString(constants.TopicFixity, gf.Identifier)
	if err != nil {
		q.Context.Logger.Errorf("Error sending '%s' to %s: %v", gf.Identifier, constants.TopicFixity, err)
		return false
	}
	q.Context.Logger.Infof("Added '%s' to %s", gf.Identifier, constants.TopicFixity)
	return true
}
