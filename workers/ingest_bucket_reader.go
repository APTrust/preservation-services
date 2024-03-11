package workers

import (
	ctx "context"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/minio/minio-go/v7"
)

type IngestBucketReader struct {
	Context *common.Context
}

func NewIngestBucketReader() *IngestBucketReader {
	return &IngestBucketReader{
		Context: common.NewContext(),
	}
}

func (r *IngestBucketReader) RunOnce() {
	r.logStartup()
	r.scanReceivingBuckets()
}

func (r *IngestBucketReader) RunAsService() {
	r.logStartup()
	for {
		r.scanReceivingBuckets()
		r.Context.Logger.Infof("Finished. Will scan again in %s",
			r.Context.Config.IngestBucketReaderInterval.String())
		time.Sleep(r.Context.Config.IngestBucketReaderInterval)
	}
}

func (r *IngestBucketReader) logStartup() {
	r.Context.Logger.Info("Starting with config settings:")
	r.Context.Logger.Info(r.Context.Config.ToJSON())
	r.Context.Logger.Infof("Scan interval: %s",
		r.Context.Config.IngestBucketReaderInterval.String())
}

func (r *IngestBucketReader) scanReceivingBuckets() {
	for _, inst := range r.LoadInstitutions() {
		// TODO: This should be fixed in Registry, but confirm.
		// Pharos needs to provide proper filtering on Institutions controller
		if inst.State == "D" {
			r.Context.Logger.Infof("Skipping inactive institution %s", inst.Identifier)
			continue
		}
		r.Context.Logger.Infof("Scanning ingest bucket for %s", inst.Identifier)
		r.ScanBucket(inst)
	}
}

func (r *IngestBucketReader) LoadInstitutions() []*registry.Institution {
	v := url.Values{}
	v.Set("page", "1")
	v.Set("per_page", "100")
	resp := r.Context.RegistryClient.InstitutionList(v)
	if resp.Error != nil {
		r.Context.Logger.Errorf("Error getting institutions from Registry: %v", resp.Error)
	}
	return resp.Institutions()
}

func (r *IngestBucketReader) ScanBucket(institution *registry.Institution) {
	s3Client := r.Context.S3Clients[constants.StorageProviderAWS]
	doneCh := make(chan struct{})
	defer close(doneCh)
	objectCh := s3Client.ListObjects(
		ctx.Background(),
		institution.ReceivingBucket,
		minio.ListObjectsOptions{
			Prefix:    "",
			Recursive: false,
		})
	for obj := range objectCh {
		if obj.Err != nil {
			r.Context.Logger.Errorf("Error reading %s: %v", institution.ReceivingBucket, obj.Err)
			continue
		}
		if !strings.HasSuffix(obj.Key, ".tar") {
			r.Context.Logger.Infof("Skipping %s: not a tar file", obj.Key)
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
		r.Context.Logger.Infof("Skipping %s: WorkItem already exists", obj.Key)
		return
	}
	r.CreateAndQueueItem(institution, obj)
}

func (r *IngestBucketReader) WorkItemAlreadyExists(instID int64, name, etag string) (bool, error) {
	v := url.Values{}
	v.Set("name", name)
	v.Set("etag", etag)
	v.Set("institution_id", strconv.FormatInt(instID, 10))
	v.Set("action", constants.ActionIngest)
	v.Set("sort", "date_processed__desc")
	v.Set("page", "1")
	v.Set("per_page", "10")
	resp := r.Context.RegistryClient.WorkItemList(v)
	if resp.Error != nil {
		return false, resp.Error
	}
	workItemExists := false
	if len(resp.WorkItems()) > 0 {
		// resp.WorkItem() is the same as resp.WorkItems()[0].
		// This is the most recent ingest work item.
		// If the work item is still in process, then we can
		// say the ingest work item exists and we don't need
		// to create another one.
		if !resp.WorkItem().ProcessingHasCompleted() {
			r.Context.Logger.Infof("Pending/running ingest work item exists for bag %s with etag %s. No need to re-ingest this one.", name, etag)
			workItemExists = true
		} else {
			// We have a completed ingest work item that exactly
			// matches the item in the receiving bucket. Same name,
			// institution and e-tag. And we know the ingest work
			// item has completed. If the object itself is still
			// active, we don't need to reingest it. If it has been
			// deleted, we do need to reingest it. This happens when
			// a depositor wants to change the storage option of an
			// object. The delete it, then re-upload the same bag
			// with a new storage option. https://trello.com/c/TE8PVrzq
			objId := resp.WorkItem().IntellectualObjectID
			workItemExists = r.ActiveObjectExists(objId)
			if workItemExists {
				r.Context.Logger.Infof("Completed ingest work item exists for bag %s with etag %s and the intellectual object (id=%d) is still active in the system. No need to re-ingest this one.", name, etag, objId)
			} else {
				r.Context.Logger.Infof("Completed ingest work item exists for bag %s with etag %s but the intellectual object (id=%d) was subsequently deleted, so we do need to re-ingest this one.", name, etag, objId)
			}
		}
	}
	return workItemExists, nil
}

func (r *IngestBucketReader) ActiveObjectExists(objId int64) bool {
	activeObjectExists := false
	resp := r.Context.RegistryClient.IntellectualObjectByID(objId)
	if resp.Error == nil && resp.IntellectualObject() != nil && resp.IntellectualObject().State == constants.StateActive {
		activeObjectExists = true
	}
	return activeObjectExists
}

func (r *IngestBucketReader) CreateAndQueueItem(institution *registry.Institution, obj minio.ObjectInfo) {
	item := r.CreateWorkItem(institution, obj)
	resp := r.Context.RegistryClient.WorkItemSave(item)
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
	resp = r.Context.RegistryClient.WorkItemSave(savedItem)
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
		DateProcessed: time.Now().UTC(),
		ETag:          strings.Replace(obj.ETag, "\"", "", -1),
		InstitutionID: institution.ID,
		Name:          obj.Key,
		Note:          "Bag is in receiving bucket",
		Outcome:       "Item is pending ingest",
		Retry:         true,
		Size:          obj.Size,
		Stage:         constants.StageReceive,
		Status:        constants.StatusPending,
		User:          "system@aptrust.org",
	}
}
