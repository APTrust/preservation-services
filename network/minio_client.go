package network

import (
	"context"
	"github.com/minio/minio-go/v6"
	//"github.com/minio/minio-go/v6/pkg/encrypt"
	"io"
	"net/url"
	"time"
)

/*
   Formally define the Minio client interface so we can mock it for testing.
   See https://docs.min.io/docs/golang-client-api-reference.html

   Note that we define object-level methods only. Our service workers need
   to put, get, and delete objects. They do not need to create buckets or
   modify bucket policies, and we don't want them to even be able to perform
   those operations.
*/

type MinioClientInterface interface {
	ComposeObject(dst minio.DestinationInfo, srcs []minio.SourceInfo) error
	CopyObject(dst minio.DestinationInfo, src minio.SourceInfo) error
	FGetObject(bucketName, objectName, filePath string, opts minio.GetObjectOptions) error
	FGetObjectWithContext(ctx context.Context, bucketName, objectName, filePath string, opts minio.GetObjectOptions) error
	FPutObject(bucketName, objectName, filePath string, opts minio.PutObjectOptions) (length int64, err error)
	FPutObjectWithContext(context.Context, string, string, string, minio.PutObjectOptions) (int64, error)
	GetObject(bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error)
	GetObjectRetention(bucketName, objectName, versionID string) (mode *minio.RetentionMode, retainUntilDate *time.Time, err error)
	GetObjectWithContext(context.Context, string, string, minio.GetObjectOptions) (*minio.Object, error)
	//GetObjectWithContext(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error)
	ListObjects(string, string, bool, <-chan struct{}) <-chan minio.ObjectInfo
	ListObjectsV2(string, string, bool, <-chan struct{}) <-chan minio.ObjectInfo
	PresignedHeadObject(bucketName, objectName string, expiry time.Duration, reqParams url.Values) (*url.URL, error)
	PresignedPutObject(bucketName, objectName string, expiry time.Duration) (*url.URL, error)
	PutObject(string, string, io.Reader, int64, minio.PutObjectOptions) (int64, error)
	PutObjectRetention(bucketName, objectName string, opts minio.PutObjectRetentionOptions) error
	PutObjectWithContext(context.Context, string, string, io.Reader, int64, minio.PutObjectOptions) (int64, error)
	RemoveIncompleteUpload(bucketName, objectName string) error
	RemoveObject(bucketName, objectName string) error
	RemoveObjectWithOptions(bucketName, objectName string, opts minio.RemoveObjectOptions) error
	RemoveObjects(string, <-chan string) <-chan minio.RemoveObjectError
	RemoveObjectsWithContext(context.Context, string, <-chan string) <-chan minio.RemoveObjectError
	SelectObjectContent(context.Context, string, string, minio.SelectObjectOptions) (*minio.SelectResults, error)
	StatObject(bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
	StatObjectWithContext(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
}
