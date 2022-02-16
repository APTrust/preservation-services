package restoration

import (
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

type BaseConstructor func(*common.Context, int, *service.RestorationObject) Runnable

type Runnable interface {
	Run() (int, []*service.ProcessingError)
}

// Base is the base type for workers in the ingest namespace.
type Base struct {
	Context           *common.Context
	RestorationObject *service.RestorationObject
	WorkItemID        int64
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

// IngestObjectGet satisfies Runnable interface. Does nothing because
// we don't work with IngestObjects in this context.
func (b *Base) IngestObjectGet() *service.IngestObject {
	return nil
}

// IngestObjectSave satisfies Runnable interface. Does nothing because
// we don't work with IngestObjects in this context.
func (b *Base) IngestObjectSave() error {
	return nil
}
