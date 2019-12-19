package testutil

import (
	"context"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/minio/minio-go/v6"
	"github.com/minio/minio-go/v6/pkg/encrypt"
	"io"
	"net/http"
)

// This file contains interface definitions that allow us to generate mocks
// for testing. See mocks/README.md

// Interface from https://github.com/minio/minio-go/blob/master/core.go
type MinioClient interface {
	ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListBucketResult, err error)
	ListObjectsV2(bucketName, objectPrefix, continuationToken string, fetchOwner bool, delimiter string, maxkeys int, startAfter string) (minio.ListBucketV2Result, error)
	CopyObjectWithContext(ctx context.Context, sourceBucket, sourceObject, destBucket, destObject string, metadata map[string]string) (minio.ObjectInfo, error)
	CopyObject(sourceBucket, sourceObject, destBucket, destObject string, metadata map[string]string) (minio.ObjectInfo, error)
	CopyObjectPartWithContext(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset, length int64, metadata map[string]string) (p minio.CompletePart, err error)
	CopyObjectPart(srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset, length int64, metadata map[string]string) (p minio.CompletePart, err error)
	PutObjectWithContext(ctx context.Context, bucket, object string, data io.Reader, size int64, md5Base64, sha256Hex string, metadata map[string]string, sse encrypt.ServerSide) (minio.ObjectInfo, error)
	PutObject(bucket, object string, data io.Reader, size int64, md5Base64, sha256Hex string, metadata map[string]string, sse encrypt.ServerSide) (minio.ObjectInfo, error)
	NewMultipartUpload(bucket, object string, opts minio.PutObjectOptions) (uploadID string, err error)
	ListMultipartUploads(bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result minio.ListMultipartUploadsResult, err error)
	PutObjectPartWithContext(ctx context.Context, bucket, object, uploadID string, partID int, data io.Reader, size int64, md5Base64, sha256Hex string, sse encrypt.ServerSide) (minio.ObjectPart, error)
	PutObjectPart(bucket, object, uploadID string, partID int, data io.Reader, size int64, md5Base64, sha256Hex string, sse encrypt.ServerSide) (minio.ObjectPart, error)
	ListObjectParts(bucket, object, uploadID string, partNumberMarker int, maxParts int) (result minio.ListObjectPartsResult, err error)
	CompleteMultipartUploadWithContext(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart) (string, error)
	CompleteMultipartUpload(bucket, object, uploadID string, parts []minio.CompletePart) (string, error)
	AbortMultipartUploadWithContext(ctx context.Context, bucket, object, uploadID string) error
	AbortMultipartUpload(bucket, object, uploadID string) error
	GetBucketPolicy(bucket string) (string, error)
	PutBucketPolicy(bucket, bucketPolicy string) error
	PutBucketPolicyWithContext(ctx context.Context, bucket, bucketPolicy string) error
	GetObjectWithContext(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, minio.ObjectInfo, http.Header, error)
	GetObject(bucketName, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, minio.ObjectInfo, http.Header, error)
	StatObjectWithContext(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
	StatObject(bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
}

// https://github.com/APTrust/preservation-services/blob/master/network/redis_client.go
// This will need to be updated as the interface evolves
type RedisClient interface {
	Ping() (string, error)
	IngestObjectGet(workItemId int, objIdentifier string) (*service.IngestObject, error)
	IngestObjectSave(workItemId int, obj *service.IngestObject) error
	IngestObjectDelete(workItemId int, objIdentifier string) error
	IngestFileGet(workItemId int, fileIdentifier string) (*service.IngestFile, error)
	IngestFileSave(workItemId int, f *service.IngestFile) error
	IngestFileDelete(workItemId int, fileIdentifier string) error
}
