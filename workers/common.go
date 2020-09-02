package workers

import (
	"fmt"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
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
		fmt.Sprintf("Rejecting WorkItem %d because it's a single-file restoration and does not belong in the bag/object restoration queue.", workItem.ID)
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
