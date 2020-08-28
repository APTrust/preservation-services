package restoration

import (
	"fmt"

	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
)

const defaultPriority = 10000

// BestRestorationSource returns the best preservation bucket from which
// to restore a file. We generally want to restore from S3 over Glacier,
// and US East over other regions. We only need to figure this out once,
// since all of an object's files will be stored in the same preservation
// bucket or buckets.
func BestRestorationSource(gf *registry.GenericFile, context *common.Context) (bestSource *common.PerservationBucket, err error) {
	priority := defaultPriority
	for _, storageRecord := range gf.StorageRecords {
		for _, preservationBucket := range context.Config.PerservationBuckets {
			if preservationBucket.HostsURL(storageRecord.URL) && preservationBucket.RestorePriority < priority {
				bestSource = preservationBucket
				priority = preservationBucket.RestorePriority
			}
		}
	}
	if priority == defaultPriority {
		err = fmt.Errorf("Could not find any suitable restoration source for %s. (%d preservation URLS, %d PerservationBuckets", gf.Identifier, len(gf.StorageRecords), len(context.Config.PerservationBuckets))
	} else {
		context.Logger.Infof("Restoring %s from %s", gf.Identifier, bestSource.Bucket)
	}
	return bestSource, err
}
