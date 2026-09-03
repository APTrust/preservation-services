package transfer

import (
	"fmt"

	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

type BaseConstructor func(*common.Context, int64, *service.TransferObject) Runnable

type Runnable interface {
	Run() (int, []*service.ProcessingError)
	TransferObjectGet() *service.TransferObject
	TransferObjectSave() error
}

// Base is the base type for workers in the transfer namespace.
type Base struct {
	Context        *common.Context
	TransferObject *service.TransferObject
	WorkItemID     int64
}

// TransferObjectGet returns this struct's TransferObject. This satisfies part
// of the Runnable interface.
func (b *Base) TransferObjectGet() *service.TransferObject {
	return b.TransferObject
}

// TransferFileGet returns a TransferFile record from Redis.
func (b *Base) TransferFileGet(identifier string) (*service.TransferFile, error) {
	transferFile, err := b.Context.RedisClient.TransferFileGet(b.WorkItemID, identifier)
	if err != nil {
		b.Context.Logger.Errorf(
			"Failed to retrieve TransferFile from redis: WorkItem %d, %s: %s",
			b.WorkItemID, identifier, err.Error())
	}
	return transferFile, err
}

// TransferFileSave saves a TransferFile to Redis.
func (b *Base) TransferFileSave(transferFile *service.TransferFile) error {
	err := b.Context.RedisClient.TransferFileSave(b.WorkItemID, transferFile)
	if err != nil {
		b.Context.Logger.Errorf(
			"Failed to save TransferFile to redis: WorkItem %d, %s: %s",
			b.WorkItemID, transferFile.Identifier(), err.Error())
	}
	return err
}

// TransferObjectSave saves a TransferObject record to Redis.
func (b *Base) TransferObjectSave() error {
	err := b.Context.RedisClient.TransferObjectSave(b.WorkItemID, b.TransferObject)
	if err != nil {
		b.Context.Logger.Errorf(
			"Failed to save TransferObject to redis: WorkItem %d, %s: %s",
			b.WorkItemID, b.TransferObject.Identifier(), err.Error())
	} else {
		b.Context.Logger.Infof(
			"Saved TransferObject to redis: WorkItem %d, %s",
			b.WorkItemID, b.TransferObject.Identifier())
	}
	return err
}

// S3KeyFor returns the S3 key for a transfer file in the bucket.
// Note that the bucket uses UUID keys, not file identifiers.
func (b *Base) S3KeyFor(transferFile *service.TransferFile) string {
	return fmt.Sprintf("%d/%s", b.WorkItemID, transferFile.UUID)
}

// Error returns a ProcessingError describing something that went wrong
// during the transfer process for this object. Identifier can be either
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
