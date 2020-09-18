package restoration

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
)

const defaultPriority = 10000

// BestRestorationSource returns the best preservation bucket from which
// to restore a file. We generally want to restore from S3 over Glacier,
// and US East over other regions. We only need to figure this out once,
// since all of an object's files will be stored in the same preservation
// bucket or buckets.
func BestRestorationSource(context *common.Context, gf *registry.GenericFile) (bestSource *common.PreservationBucket, storageRecord *registry.StorageRecord, err error) {
	priority := defaultPriority
	for _, sr := range gf.StorageRecords {
		for _, preservationBucket := range context.Config.PreservationBuckets {
			if preservationBucket.HostsURL(sr.URL) && preservationBucket.RestorePriority < priority {
				bestSource = preservationBucket
				storageRecord = sr
				priority = preservationBucket.RestorePriority
			}
		}
	}
	if priority == defaultPriority {
		err = fmt.Errorf("Could not find any suitable restoration source for %s. (%d preservation URLS, %d PreservationBuckets)", gf.Identifier, len(gf.StorageRecords), len(context.Config.PreservationBuckets))
	} else {
		context.Logger.Infof("Most accessible source for %s is %s", gf.Identifier, bestSource.Bucket)
	}
	return bestSource, storageRecord, err
}

// GetBatchOfFiles returns a batch of GenericFile records from Pharos.
func GetBatchOfFiles(context *common.Context, objectIdentifier string, pageNumber int) (genericFiles []*registry.GenericFile, err error) {
	params := url.Values{}
	params.Set("intellectual_object_identifier", objectIdentifier)
	params.Set("page", strconv.Itoa(pageNumber))
	params.Set("per_page", strconv.Itoa(batchSize))
	params.Set("sort", "name")
	params.Set("state", "A")
	params.Set("include_storage_records", "true")
	resp := context.PharosClient.GenericFileList(params)
	return resp.GenericFiles(), resp.Error
}
