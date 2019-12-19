package network

import (
	"context"
	"github.com/minio/minio-go/v6"
	"github.com/minio/minio-go/v6/pkg/encrypt"
	"io"
	"net/http"
)

// Formally define the Minio client interface so we can mock it for testing.
// See https://github.com/minio/minio-go/blob/master/core.go for the basics,
// and https://docs.min.io/docs/golang-client-api-reference.html for how
// Client differs from Core.

type MinioClientInterface interface {
	ListObjects(bucket, prefix, marker, delimiter string, maxKeys int) (result minio.ListBucketResult, err error)
	ListObjectsV2(bucketName, objectPrefix, continuationToken string, fetchOwner bool, delimiter string, maxkeys int, startAfter string) (minio.ListBucketV2Result, error)
	CopyObjectWithContext(ctx context.Context, sourceBucket, sourceObject, destBucket, destObject string, metadata map[string]string) (minio.ObjectInfo, error)
	CopyObject(sourceBucket, sourceObject, destBucket, destObject string, metadata map[string]string) (minio.ObjectInfo, error)
	CopyObjectPartWithContext(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset, length int64, metadata map[string]string) (p minio.CompletePart, err error)
	CopyObjectPart(srcBucket, srcObject, destBucket, destObject string, uploadID string, partID int, startOffset, length int64, metadata map[string]string) (p minio.CompletePart, err error)
	FPutObject(bucketName, objectName, filePath string, opts minio.PutObjectOptions) (length int64, err error)
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
	GetObject(bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error)
	StatObjectWithContext(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
	StatObject(bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
}
