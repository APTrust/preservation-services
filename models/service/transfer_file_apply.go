package service

// TransferFileApplyFn defines the type signature for functions passed
// to RedisClient.TransferFilesApply.
type TransferFileApplyFn = func(transferFile *TransferFile) (errors []*ProcessingError)

// TransferFileApplyOptions describe a function to be applied to all
// TransferFiles in a collection. For now, this is done through the
// RedisClient's TransferFileForeach method.
type TransferFileApplyOptions struct {

	// MaxErrors is the maximum number of error to allow before
	// TransferFilesForeach returns. In most cases, this should be
	// 1, but when copying files, it should be set higher (10, 30, 50)
	// because we expect a few files to fail in a batch of several thousand,
	// and we want to finish as many uploads as possible before trying again later.
	MaxErrors int

	// MaxRetries is the maximum number of times to re-run Fn if it
	// produces errors. This should usually be set to 1, except when
	// copying files to target buckets, where transient
	// errors like "Connection reset" are common, and retries almost
	// always fix the problem.
	MaxRetries int

	// RetryMs is the amount of time to wait between retries.
	RetryMs int

	// SaveChanges indicates whether changes made to the TransferFile
	// by Fn should be written back to Redis. In most cases, when we're
	// changing attributes of TransferFile, we do want to save these changes.
	// If Fn performs read-only operations in TransferFile, setting this
	// to false can save a lot of overhead by avoiding write calls to
	// Redis.
	SaveChanges bool

	// WorkItemID is the ID of the WorkItem with which the transfer files
	// are associated.
	WorkItemID int64
}
