package service

// IngestFileApplyOptions describe a function to be applied to all
// IngestFiles in a collection. For now, this is done through the
// RedisClient's IngestFileForeach method.
type IngestFileApplyOptions struct {

	// MaxErrors is the maximum number of error to allow before
	// IngestFilesForeach returns. In most cases, this should be
	// 1, but when uploading files to staging or preservation, it
	// should be set higher (10, 30, 50) because we expect a few files
	// to fail in a batch of several thousand, and we want to finish
	// as many uploads as possible before trying again later.
	MaxErrors int

	// MaxRetries is the maximum number of times to re-run Fn if it
	// produces errors. This should usually be set to 1, except when
	// uploading files to S3 staging or preservation, where transient
	// errors like "Connection reset" are common, and retries almost
	// always fix the problem.
	MaxRetries int

	// RetryMs is the amount of time to wait between retries.
	RetryMs int

	// SaveChanges indicates whether changes made to the IngestFile
	// by Fn should be written back to Redis. In most cases, when we're
	// changing attributes of IngestFile, we do want to save these changes.
	// If Fn performs read-only operations in IngestFile, setting this
	// to false can save a lot of overhead by avoiding write calls to
	// Redis.
	SaveChanges bool

	// WorkItemID is the ID of the WorkItem with which the ingest files
	// are associated.
	WorkItemID int
}
