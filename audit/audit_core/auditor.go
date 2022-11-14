package audit_core

import (
	ctx "context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/restoration"
	"github.com/minio/minio-go/v7"
)

type Auditor struct {
	Context                *common.Context
	GenericFileID          int64
	DoFullCheckIfNecessary bool
}

// NewAuditor creates a new Auditor.
func NewAuditor(context *common.Context, gfId int64, doFullCheckIfNecessary bool) *Auditor {
	return &Auditor{
		Context:                context,
		GenericFileID:          gfId,
		DoFullCheckIfNecessary: doFullCheckIfNecessary,
	}
}

func (a *Auditor) Run() *AuditRecord {
	record := NewAuditRecord(a.GenericFileID)
	record.CheckStartedAt = time.Now()
	gf, err := a.GetGenericFile()
	if err != nil {
		record.Error = err.Error()
		return record
	}
	record.GenericFileIdentifier = gf.Identifier
	record.GenericFileCreatedAt = gf.CreatedAt
	record.GenericFileUpdatedAt = gf.UpdatedAt
	record.RegistrySize = gf.Size
	record.IsGlacierOnlyFile = a.IsGlacierOnlyFile(gf)

	checksumMd5 := gf.GetLatestChecksum(constants.AlgMd5)
	if checksumMd5 != nil {
		record.RegistryMd5 = checksumMd5.Digest
	}

	checksumSha256 := gf.GetLatestChecksum(constants.AlgSha256)
	if checksumSha256 != nil {
		record.RegistrySha256 = checksumSha256.Digest
	}

	preservationBucket, storageRecord, err := restoration.BestRestorationSource(a.Context, gf)
	if err != nil {
		record.Error = fmt.Sprintf("Could not find restoration source for: %v", err)
		return record
	}
	record.PreservationUrl = storageRecord.URL

	client := a.Context.S3Clients[preservationBucket.Bucket]
	if client == nil {
		record.Error = fmt.Sprintf("Cannot find S3 client for provider %s", preservationBucket.Provider)
		return record
	}

	s3Stats, err := client.StatObject(
		ctx.Background(),
		preservationBucket.Bucket,
		gf.UUID,
		minio.GetObjectOptions{},
	)
	if err != nil {
		record.Error = fmt.Sprintf("Could not stat file at %s/%s: %v", preservationBucket.Bucket, gf.UUID, err)
		return record
	}

	record.S3Etag = s3Stats.ETag
	record.S3Size = s3Stats.Size

	record.S3MetaMd5 = s3Stats.Metadata.Get("x-amz-meta-md5")
	record.S3MetaSha256 = s3Stats.Metadata.Get("x-amz-meta-sha256")
	record.S3MetaInstitution = s3Stats.Metadata.Get("x-amz-meta-institution")
	record.S3MetaBagName = s3Stats.Metadata.Get("x-amz-meta-bag")
	record.S3MetaPathInBag = s3Stats.Metadata.Get("x-amz-meta-bagpath")
	if record.S3MetaPathInBag == "" {
		// bag path is encoded for Wasabi
		record.S3MetaPathInBag = s3Stats.Metadata.Get("x-amz-meta-bagpath-encoded")
	}

	if a.HasMetadataMismatch(record, gf) {
		record.ReasonForCheck = "Metadata mismatch"
	} else if !record.NeedsFixityCheck() {
		record.CheckPassed = true
		record.Method = QuickMatch
		record.CheckCompletedAt = time.Now()
		return record
	}

	// We have a mismatch, but stop here if user doesn't want to do
	// a full fixity check.
	if !a.DoFullCheckIfNecessary {
		record.CheckPassed = false
		record.CheckCompletedAt = time.Now()
		return record
	}

	record.Method = FullFixity

	if record.IsGlacierOnlyFile {
		record.NeedsGlacierFixityCheck = true
		return record
	}

	actualFixity, err := a.CalculateFixity(gf, preservationBucket)
	if err != nil {
		record.Error = fmt.Sprintf("Error trying to calculate fixity: %v", err)
		return record
	}
	record.StreamSha256 = actualFixity
	if record.RegistrySha256 != actualFixity {
		record.CheckPassed = false
	}
	record.CheckCompletedAt = time.Now()
	return record
}

func (a *Auditor) HasMetadataMismatch(record *AuditRecord, gf *registry.GenericFile) bool {
	gfPath, err := gf.PathInBag()
	if err != nil {
		record.MismatchedMetaPath = (record.S3MetaPathInBag != gfPath && record.S3MetaPathInBag != url.PathEscape(gfPath))
	}
	objIdentifier, _ := gf.IntellectualObjectIdentifier()
	record.MismatchedMetaBagName = record.S3MetaBagName != objIdentifier
	record.MismatchedMetaInstitution = record.S3MetaInstitution != gf.InstitutionIdentifier()
	record.MismatchedMetaMd5 = record.S3MetaMd5 != record.RegistryMd5
	record.MismatchedMetaSha256 = record.S3MetaSha256 != record.RegistrySha256
	return record.HasMetadataMismatch()
}

func (a *Auditor) GetGenericFile() (*registry.GenericFile, error) {
	resp := a.Context.RegistryClient.GenericFileByID(a.GenericFileID)
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.GenericFile(), nil
}

func (a *Auditor) IsGlacierOnlyFile(gf *registry.GenericFile) bool {
	return strings.HasPrefix(gf.StorageOption, "Glacier")
}

func (a *Auditor) CalculateFixity(gf *registry.GenericFile, preservationBucket *common.PreservationBucket) (fixity string, err error) {
	client := a.Context.S3Clients[preservationBucket.Bucket]
	if client == nil {
		return "", fmt.Errorf("Cannot find S3 client for provider %s", preservationBucket.Provider)
	}
	obj, err := client.GetObject(
		ctx.Background(),
		preservationBucket.Bucket,
		gf.UUID,
		minio.GetObjectOptions{},
	)
	if err != nil {
		return "", fmt.Errorf("Error getting %s from bucket %s: %v", gf.UUID, preservationBucket.Bucket, err)
	}
	defer obj.Close()

	sha256Hash := sha256.New()
	_, err = io.Copy(sha256Hash, obj)
	if err != nil {
		return "", fmt.Errorf("Error streaming S3 file %s/%s through hash function: %v", preservationBucket.Bucket, gf.UUID, err)
	}
	fixity = fmt.Sprintf("%x", sha256Hash.Sum(nil))
	return fixity, err
}
