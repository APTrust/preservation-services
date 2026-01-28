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
	perPage := util.Min(500, q.Context.Config.MaxFixityItemsPerRun)
	params := url.Values{}
	itemsAdded := 0
	params.Set("per_page", strconv.Itoa(perPage))
	params.Set("page", "1")
	params.Add("storage_option__in", constants.StorageStandard)
	params.Add("state", constants.StateActive)
	params.Set("sort", "last_fixity_check")

	hours := q.Context.Config.MaxDaysSinceFixityCheck * 24 * -1
	sinceWhen := time.Now().Add(time.Duration(hours) * time.Hour).UTC()
	params.Set("last_fixity_check__lteq", sinceWhen.Format(time.RFC3339))

	// This seemingly unnecessary filter address the long-standing
	// slow query problem described in ticket https://trello.com/c/KlrtsAXo
	//
	// Unless we put a lower bound in the last fixity check date range,
	// we miss the index scan and do a full table scan. Without earliestDate,
	// this query takes around 14 seconds in production. With it, it takes
	// 82 milliseconds.
	//
	// See the comment from Goyal at
	// https://stackoverflow.com/questions/5203755/why-does-postgresql-perform-sequential-scan-on-indexed-column
	earliestDate := sinceWhen.Add(-30 * 24 * time.Hour)
	params.Set("last_fixity_check__gteq", earliestDate.Format(time.RFC3339))

	q.Context.Logger.Infof("Queuing up to %d files not checked since %s to topic %s", q.Context.Config.MaxFixityItemsPerRun, sinceWhen.Format(time.RFC3339), constants.TopicFixity)
	q.Context.Logger.Infof("Set dates for fixity query %s - %s", earliestDate.Format(time.RFC3339), sinceWhen.Format(time.RFC3339))

	for {
		resp := q.Context.RegistryClient.GenericFileList(params)
		if resp == nil {
			q.Context.Logger.Errorf("Registry response is nil for params %v", params)
			continue
		}
		if resp.Request != nil && resp.Request.URL != nil {
			q.Context.Logger.Infof("GET %s", resp.Request.URL)
		}
		if resp.Error != nil {
			q.Context.Logger.Errorf("Error getting GenericFile list from Registry: %s", resp.Error)
		}
		for _, gf := range resp.GenericFiles() {
			if q.addToNSQ(gf) {
				itemsAdded += 1
			}
		}
		if !resp.HasNextPage() || itemsAdded >= q.Context.Config.MaxFixityItemsPerRun {
			break
		}
		params = resp.ParamsForNextPage()
	}
}

func (q *QueueFixity) queueOne() {
	resp := q.Context.RegistryClient.GenericFileByIdentifier(q.Identifier)
	if resp.Error != nil {
		q.Context.Logger.Errorf("Error getting GenericFile list from Registry: %s", resp.Error)
		return
	}
	q.addToNSQ(resp.GenericFile())
}

func (q *QueueFixity) addToNSQ(gf *registry.GenericFile) bool {
	err := q.Context.NSQClient.Enqueue(constants.TopicFixity, gf.ID)
	if err != nil {
		q.Context.Logger.Errorf("Error sending '%s' (%d) to %s: %v", gf.Identifier, gf.ID, constants.TopicFixity, err)
		return false
	}
	q.Context.Logger.Infof("Added '%s' (%d) to %s", gf.Identifier, gf.ID, constants.TopicFixity)
	return true
}
