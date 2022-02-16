package restoration

import (
	ctx "context"
	"fmt"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/minio/minio-go/v7"
)

// FileRestorer restores individual files to a depositor's restoration bucket.
type FileRestorer struct {
	Base
}

// NewFileRestorer creates a new FileRestorer to copy files from S3
// to local disk for packaging.
func NewFileRestorer(context *common.Context, workItemID int64, restorationObject *service.RestorationObject) *FileRestorer {
	return &FileRestorer{
		Base: Base{
			Context:           context,
			RestorationObject: restorationObject,
			WorkItemID:        workItemID,
		},
	}
}

// Run restores the file to the depositor's restoration bucket.
func (r *FileRestorer) Run() (fileCount int, errors []*service.ProcessingError) {
	gf, err := r.getGenericFile()
	if err != nil {
		errors = append(errors, r.Error(r.RestorationObject.Identifier, err, false))
		return fileCount, errors
	}
	obj, err := r.getFileFromPreservation(gf)
	if err != nil {
		errors = append(errors, r.Error(gf.Identifier, err, false))
		return fileCount, errors
	}
	defer obj.Close()
	r.Context.Logger.Infof("Copying %s to %s", gf.Identifier, r.RestorationObject.RestorationTarget)
	_, err = r.Context.S3Clients[constants.StorageProviderAWS].PutObject(
		ctx.Background(),
		r.RestorationObject.RestorationTarget,
		gf.Identifier,
		obj,
		gf.Size,
		minio.PutObjectOptions{})
	if err != nil {
		errors = append(errors, r.Error(gf.Identifier, err, false))
	}
	if len(errors) == 0 {
		fileCount = 1
		r.RestorationObject.AllFilesRestored = true
		r.RestorationObject.URL = fmt.Sprintf("%s%s/%s", constants.AWSBucketPrefix, r.RestorationObject.RestorationTarget, r.RestorationObject.Identifier)
	}
	return fileCount, errors
}

// Get the GenericFile record from Pharos
func (r *FileRestorer) getGenericFile() (*registry.GenericFile, error) {
	resp := r.Context.RegistryClient.GenericFileByIdentifier(r.RestorationObject.Identifier)
	if resp.Error != nil {
		return nil, resp.Error
	}
	gf := resp.GenericFile()
	if gf == nil {
		return nil, fmt.Errorf("Pharos returned nil for file %s", r.RestorationObject.Identifier)
	}
	r.Context.Logger.Infof("File %s has %d storage records", gf.Identifier, len(gf.StorageRecords))
	return gf, nil
}

// Get the S3 object from preservation storage.
func (r *FileRestorer) getFileFromPreservation(gf *registry.GenericFile) (*minio.Object, error) {
	b, _, err := BestRestorationSource(r.Context, gf)
	if err != nil {
		return nil, err
	}
	client := r.Context.S3Clients[b.Provider]
	obj, err := client.GetObject(ctx.Background(), b.Bucket, gf.UUID, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	r.Context.Logger.Infof("Found %s in bucket %s", gf.Identifier, b.Bucket)
	return obj, err
}
