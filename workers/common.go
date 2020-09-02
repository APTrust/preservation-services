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
