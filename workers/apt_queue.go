package workers

import (
	"fmt"
	"net/url"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/network"
)

type APTQueue struct {
	Context *common.Context
}

// NewAPTQueue creates a new queue worker to push WorkItems from
// Registry into NSQ, and marked them as queued.
func NewAPTQueue() *APTQueue {
	return &APTQueue{
		Context: common.NewContext(),
	}
}

func (q *APTQueue) RunOnce() {
	q.logStartup()
	q.run()
}

func (q *APTQueue) RunAsService() {
	q.logStartup()
	for {
		q.run()
		time.Sleep(q.Context.Config.APTQueueInterval)
	}
}

func (q *APTQueue) logStartup() {
	q.Context.Logger.Info("Starting with config settings:")
	q.Context.Logger.Info(q.Context.Config.ToJSON())
	q.Context.Logger.Infof("Scan interval: %s",
		q.Context.Config.APTQueueInterval.String())
}

// Run retrieves all unqueued work items from Registry and pushes
// them into the appropriate NSQ topic.
//
// NOTE: apt_queue is a legacy worker from the Pharos days. Pharos
// did not automatically add new restoration and deletion requests
// to NSQ, so this worker scanned the WorkItems list for new
// restorations and deletions, then queued them itself.
//
// Registry now queues restorations as soon as they're created, and
// it queues deletions as soon as they're approved, so there is no
// need for this worker. In fact, in our live environments, it's been
// double-queuing items and causing problems.
//
// The only place we still need this is in the end-to-end tests, where
// we create some restoration and deletion requests that do need to be
// queued.
//
// In short, this worker should run ONLY in end-to-end tests.
func (q *APTQueue) run() {
	if !q.Context.Config.IsE2ETest() {
		q.Context.Logger.Info("apt_queue invoked but is doing nothing because this service is probably no longer necessary, except in end-to-end tests.")
		q.Context.Logger.Info("We are evaluating whether or not to remove this service entirely from our production environment.")
		return
	}
	params := url.Values{}
	params.Set("queued_at__is_null", "true")
	params.Set("status", constants.StatusPending)
	params.Set("retry", "true")
	params.Add("action__in", constants.ActionDelete)
	params.Add("action__in", constants.ActionGlacierRestore)
	params.Add("action__in", constants.ActionRestoreFile)
	params.Add("action__in", constants.ActionRestoreObject)
	params.Set("node__is_null", "true")
	params.Set("page", "1")
	params.Set("per_page", "100")
	for {
		resp := q.Context.RegistryClient.WorkItemList(params)
		if resp.Error != nil {
			q.Context.Logger.Errorf("Error getting WorkItem list from Registry: %s", resp.Error)
		}
		q.Context.Logger.Infof("Found %d items", len(resp.WorkItems()))
		for _, item := range resp.WorkItems() {
			if q.addToNSQ(item) {
				q.markAsQueued(item)
			}
		}
		q.Context.Logger.Info("HasNextPage =", resp.HasNextPage())
		if !resp.HasNextPage() {
			break
		}
		params = resp.ParamsForNextPage()
	}
}

func (q *APTQueue) addToNSQ(workItem *registry.WorkItem) bool {
	identifier := workItem.Name
	if workItem.ObjectIdentifier != "" {
		identifier = workItem.ObjectIdentifier
	}
	if workItem.GenericFileIdentifier != "" {
		identifier = workItem.GenericFileIdentifier
	}

	topic, err := constants.TopicFor(workItem.Action, workItem.Stage, workItem.GenericFileIdentifier)
	if err != nil {
		q.Context.Logger.Errorf(
			"Unknown topic for WorkItem %d - %s (%s/%s/%s)",
			workItem.ID, identifier, workItem.Action,
			workItem.Stage, workItem.Status)
		return false
	}
	err = q.Context.NSQClient.Enqueue(topic, workItem.ID)
	if err != nil {
		q.Context.Logger.Errorf("Error sending WorkItem %d %s (%s/%s/%s) - to %s: %v",
			workItem.ID, identifier, workItem.Action,
			workItem.Stage, workItem.Status, topic, err)
		return false
	}
	q.Context.Logger.Infof("Added WorkItem id %d - %s (%s/%s/%s) - to %s",
		workItem.ID, identifier, workItem.Action, workItem.Stage, workItem.Status, topic)
	return true
}

func (q *APTQueue) markAsQueued(workItem *registry.WorkItem) *registry.WorkItem {
	utcNow := time.Now().UTC()
	workItem.DateProcessed = utcNow
	workItem.QueuedAt = utcNow
	resp := q.Context.RegistryClient.WorkItemSave(workItem)
	if resp.Error != nil {
		q.Context.Logger.Error("Error setting QueuedAt for WorkItem with id %d: %v",
			workItem.ID, resp.Error)
		return nil
	}
	if resp.Response.StatusCode != 200 {
		q.processRegistryError(resp)
		return nil
	}
	q.Context.Logger.Infof("Marked WorkItem id %d (%s/%s/%s) as queued in Registry",
		workItem.ID, workItem.Action, workItem.Stage, workItem.Status)
	return resp.WorkItem()
}

func (q *APTQueue) processRegistryError(resp *network.RegistryResponse) {
	respBody := ""
	bytesRead, aptQueuer := resp.RawResponseData()
	if aptQueuer == nil {
		respBody = string(bytesRead)
	} else {
		respBody = fmt.Sprintf("[Could not read response body: %v]", aptQueuer)
	}
	q.Context.Logger.Error("%s %s returned status code %d. Response body: %s",
		resp.Request.Method, resp.Request.URL, resp.Response.StatusCode, respBody)
}
