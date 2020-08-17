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
	WorkItemID        int
}
