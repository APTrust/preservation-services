package restoration

import (
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

type BaseConstructor func(*common.Context, int, *service.RestorationObject) Runnable

type Runnable interface {
	Run() (int, []*service.ProcessingError)
	RestorationObjectGet() *service.RestorationObject
	RestorationObjectSave() error
}

// Base is the base type for workers in the ingest namespace.
type Base struct {
	Context           *common.Context
	RestorationObject *service.RestorationObject
	WorkItemID        int
}

// RestorationObjectGet returns this struct's RestorationObject. This satisfies part
// of the Runnable interface.
func (b *Base) RestorationObjectGet() *service.RestorationObject {
	return b.RestorationObject
}

// RestorationObjectSave saves an RestorationObject record to Redis.
func (b *Base) RestorationObjectSave() error {
	err := b.Context.RedisClient.RestorationObjectSave(b.WorkItemID, b.RestorationObject)
	if err != nil {
		b.Context.Logger.Errorf(
			"Failed to save RestorationObject to redis: WorkItem %d, %s: %s",
			b.WorkItemID, b.RestorationObject.Identifier, err.Error())
	} else {
		b.Context.Logger.Infof(
			"Saved RestorationObject to redis: WorkItem %d, %s",
			b.WorkItemID, b.RestorationObject.Identifier)
	}
	return err
}

// Error returns a ProcessingError describing something that went wrong
// during the restoration process for this object. Identifier can be either
// an IntellectualObect identifier, a GenericFile identifier, or in rare
// cases a WorkItem ID. Since each has a different format, you can discern
// the identifier type by looking at it.
func (b *Base) Error(identifier string, err error, isFatal bool) *service.ProcessingError {
	return service.NewProcessingError(
		b.WorkItemID,
		identifier,
		err.Error(),
		isFatal,
	)
}
