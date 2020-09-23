package workers

import (
	"fmt"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
)

// HasWrongAction returns true and marks this item as no longer in
// progress if the WorkItem.Action is anything other than the
// expected action
func HasWrongAction(context *common.Context, workItem *registry.WorkItem, expectedAction string) bool {
	if workItem.Action != expectedAction {
		message := fmt.Sprintf("Rejecting WorkItem %d because action is %s, not '%s'", workItem.ID, workItem.Action, expectedAction)
		workItem.Retry = false
		workItem.MarkNoLongerInProgress(
			workItem.Stage,
			constants.StatusCancelled,
			message,
		)
		context.Logger.Info(message)
		return true
	}
	return false
}

// IsWrongRestorationType returns true if this item does not match the
// expected restoration type. This item actually belongs
// in the object restoration queue, not the file restoration queue. Bag
// restoration items have only an ObjectIdentifier. File restorations have
// a GenericFileIdentifier.
func IsWrongRestorationType(context *common.Context, workItem *registry.WorkItem, expectedType string) bool {
	var message string
	if expectedType == constants.RestorationTypeFile && workItem.GenericFileIdentifier == "" {
		message = fmt.Sprintf("Rejecting WorkItem %d because it's an object restoration and does not belong in the file restoration queue.", workItem.ID)
	} else if expectedType == constants.RestorationTypeObject && workItem.GenericFileIdentifier != "" {
		message = fmt.Sprintf("Rejecting WorkItem %d because it's a single-file restoration and does not belong in the bag/object restoration queue.", workItem.ID)
	}
	if message != "" {
		workItem.Retry = false
		workItem.MarkNoLongerInProgress(
			workItem.Stage,
			constants.StatusCancelled,
			message,
		)
		context.Logger.Info(message)
		return true
	}
	return false
}

func GetRestorationObject(context *common.Context, workItem *registry.WorkItem, restorationSource string) (*service.RestorationObject, error) {
	resp := context.PharosClient.IntellectualObjectGet(workItem.ObjectIdentifier)
	if resp.Error != nil {
		return nil, resp.Error
	}
	intelObj := resp.IntellectualObject()
	if intelObj == nil {
		return nil, fmt.Errorf("Pharos returned nil for IntellectualObject %s", workItem.ObjectIdentifier)
	}
	resp = context.PharosClient.InstitutionGet(intelObj.Institution)
	if resp.Error != nil {
		return nil, resp.Error
	}
	institution := resp.Institution()
	if intelObj == nil {
		return nil, fmt.Errorf("Pharos returned nil for Institution %s", intelObj.Institution)
	}

	restorationType := constants.RestorationTypeObject
	identifier := workItem.ObjectIdentifier
	if workItem.GenericFileIdentifier != "" {
		restorationType = constants.RestorationTypeFile
		identifier = workItem.GenericFileIdentifier
	}

	return &service.RestorationObject{
		Identifier:             identifier,
		BagItProfileIdentifier: intelObj.BagItProfileIdentifier,
		RestorationSource:      restorationSource,
		RestorationTarget:      institution.RestoreBucket,
		RestorationType:        restorationType,
	}, nil
}

// QueueE2EWorkItem queues a WorkItem for post tests if the env variable
// APT_E2E is set to "true".
func QueueE2EWorkItem(context *common.Context, topic string, workItemID int) {
	if context.Config.IsE2ETest() {
		err := context.NSQClient.Enqueue(topic, workItemID)
		if err != nil {
			context.Logger.Errorf("E2E Queue Error %s/%d: %v", topic, workItemID, err)
		}
	}
}

// QueueE2EWorkItem queues a generic file identifier for post tests if the
// env variable APT_E2E is set to "true".
func QueueE2EIdentifier(context *common.Context, topic, identifier string) {
	if context.Config.IsE2ETest() {
		err := context.NSQClient.EnqueueString(topic, identifier)
		if err != nil {
			context.Logger.Errorf("E2E Queue Error %s/%d: %v", topic, identifier, err)
		}
	}
}
