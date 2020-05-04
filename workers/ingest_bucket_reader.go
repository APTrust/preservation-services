package workers

import (
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/minio/minio-go/v6"
)

// TODO: Create pid file and exit if other bucket reader is running

type IngestBucketReader struct {
	Context *common.Context
}

func NewIngestBucketReader(context *common.Context) *IngestBucketReader {
	return &IngestBucketReader{
		Context: context,
	}
}

func (r *IngestBucketReader) Run() {
	for _, inst := range r.LoadInstitutions() {
		r.ScanBucket(inst)
	}
}

func (r *IngestBucketReader) LoadInstitutions() []*registry.Institution {
	v := url.Values{}
	v.Set("page", "1")
	v.Set("per_page", "100")
	resp := r.Context.PharosClient.InstitutionList(v)
	if resp.Error != nil {
		r.Context.Logger.Fatalf("Error getting institutions from Pharos: %v", resp.Error)
	}
	return resp.Institutions()
}

func (r *IngestBucketReader) ScanBucket(institution *registry.Institution) {
	s3Client := r.Context.S3Clients[constants.StorageProviderAWS]
	doneCh := make(chan struct{})
	defer close(doneCh)
	objectCh := s3Client.ListObjectsV2(institution.ReceivingBucket, "", false, doneCh)
	for obj := range objectCh {
		if obj.Err != nil {
			r.Context.Logger.Errorf("Error reading %s: %v", institution.ReceivingBucket, obj.Err)
			continue
		}
		r.ProcessItem(institution, obj)
	}
}

func (r *IngestBucketReader) ProcessItem(institution *registry.Institution, obj minio.ObjectInfo) {
	exists, err := r.WorkItemAlreadyExists(institution.ID, obj.Key, obj.ETag)
	if err != nil {
		r.Context.Logger.Errorf("Error checking for existing WorkItems: %v", err)
		return
	}
	if exists {
		r.Context.Logger.Info("Skipping %s: WorkItem already exists", obj.Key)
		return
	}
	r.CreateAndQueueItem(institution, obj)
}

func (r *IngestBucketReader) WorkItemAlreadyExists(instID int, name, etag string) (bool, error) {
	v := url.Values{}
	v.Set("name", name)
	v.Set("etag", etag)
	v.Set("institution_id", strconv.Itoa(instID))
	v.Set("page", "1")
	v.Set("per_page", "100")
	resp := r.Context.PharosClient.WorkItemList(v)
	if resp.Error != nil {
		return false, resp.Error
	}
	// Pharos doesn't have good filtering for this, so we do it here.
	exists := false
	for _, item := range resp.WorkItems() {
		if item.Status != constants.StatusCancelled {
			exists = true
			break
		}
	}
	return exists, nil
}

func (r *IngestBucketReader) CreateAndQueueItem(institution *registry.Institution, obj minio.ObjectInfo) {
	item := r.CreateWorkItem(institution, obj)
	resp := r.Context.PharosClient.WorkItemSave(item)
	if resp.Error != nil {
		r.Context.Logger.Errorf("Error saving WorkItem for %s: %v", obj.Key, resp.Error)
		return
	}
	savedItem := resp.WorkItem() // item now has an ID
	err := r.Context.NSQClient.Enqueue(constants.IngestPreFetch, savedItem.ID)
	if err != nil {
		r.Context.Logger.Errorf("Error queueing WorkItem %d: %v", savedItem.ID, err)
		return
	}
	savedItem.QueuedAt = time.Now().UTC()
	resp = r.Context.PharosClient.WorkItemSave(savedItem)
	if resp.Error != nil {
		r.Context.Logger.Errorf("Error marking WorkItem %d as queued: %v", savedItem.ID, resp.Error)
		return
	}
	r.Context.Logger.Infof("Created and queued WorkItem %d for %s/%s", savedItem.ID, institution.ReceivingBucket, obj.Key)
}

func (r *IngestBucketReader) CreateWorkItem(institution *registry.Institution, obj minio.ObjectInfo) *registry.WorkItem {
	return &registry.WorkItem{
		Action:        constants.ActionIngest,
		BagDate:       obj.LastModified,
		Bucket:        institution.ReceivingBucket,
		Date:          time.Now().UTC(),
		ETag:          strings.Replace(obj.ETag, "\"", "", -1),
		InstitutionID: institution.ID,
		Name:          obj.Key,
		Note:          "Bag is in receiving bucket",
		Outcome:       "Item is pending ingest",
		Retry:         true,
		Size:          obj.Size,
		Stage:         constants.StageReceive,
		Status:        constants.StatusPending,
	}
}

func (r *IngestBucketReader) AddToNSQ(workItemID int) {

}
