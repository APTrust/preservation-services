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
// Pharos into NSQ, and marked them as queued.
func NewAPTQueue(context *common.Context) *APTQueue {
	return &APTQueue{
		Context: context,
	}
}

// Run retrieves all unqueued work items from Pharos and pushes
// them into the appropriate NSQ topic.
func (q *APTQueue) Run() {
	params := url.Values{}
	params.Set("queued", "false")
	params.Set("status", constants.StatusPending)
	params.Set("retry", "true")
	params.Set("node_empty", "true")
	params.Set("page", "1")
	params.Set("per_page", "100")
	for {
		resp := q.Context.PharosClient.WorkItemList(params)
		q.Context.Logger.Info("GET %s", resp.Request.URL)
		if resp.Error != nil {
			q.Context.Logger.Error("Error getting WorkItem list from Pharos: %s", resp.Error)
		}
		for _, item := range resp.WorkItems() {
			if q.addToNSQ(item) {
				q.markAsQueued(item)
			}
		}
		if resp.HasNextPage() == false {
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
		q.Context.Logger.Error(
			"Unknown topic for WorkItem %d - %s (%s/%s/%s)",
			workItem.ID, identifier, workItem.Action,
			workItem.Stage, workItem.Status)
		return false
	}
	err = q.Context.NSQClient.Enqueue(topic, workItem.ID)
	if err != nil {
		q.Context.Logger.Error("Error sending WorkItem %d %s (%s/%s/%s) - to %s: %v",
			workItem.ID, identifier, workItem.Action,
			workItem.Stage, workItem.Status, topic, err)
		return false
	}
	q.Context.Logger.Info("Added WorkItem id %d - %s (%s/%s/%s) - to %s",
		workItem.ID, identifier, workItem.Action, workItem.Stage, workItem.Status, topic)
	return true
}

func (q *APTQueue) markAsQueued(workItem *registry.WorkItem) *registry.WorkItem {
	utcNow := time.Now().UTC()
	workItem.Date = utcNow
	workItem.QueuedAt = utcNow
	resp := q.Context.PharosClient.WorkItemSave(workItem)
	if resp.Error != nil {
		q.Context.Logger.Error("Error setting QueuedAt for WorkItem with id %d: %v",
			workItem.ID, resp.Error)
		return nil
	}
	if resp.Response.StatusCode != 200 {
		q.processPharosError(resp)
		return nil
	}
	q.Context.Logger.Info("Marked WorkItem id %d (%s/%s/%s) as queued in Pharos",
		workItem.ID, workItem.Action, workItem.Stage, workItem.Status)
	return resp.WorkItem()
}

func (q *APTQueue) processPharosError(resp *network.PharosResponse) {
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
