package common

import (
	ctx "context"
	"fmt"

	"github.com/APTrust/preservation-services/network"
	"github.com/APTrust/preservation-services/util/logger"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/op/go-logging"
)

// Will also need a Glacier client, since Minio does not support
// Glacier operations like restore. Since we need only 2 or 3
// calls, it may be better to write our own Glacier client than
// to work with that horrid AWS library.

type Context struct {
	Config         *Config
	Logger         *logging.Logger
	NSQClient      *network.NSQClient
	RedisClient    *network.RedisClient
	RegistryClient *network.RegistryClient

	// S3Clients is a map of S3 clients, where key
	// is either a generic host name like "s3.amazonaws.com"
	// or a specific bucket name, like "aptrust.preservation.oregon".
	// We use bucket name keys only when writing to and reading from
	// long-term preservation buckets.
	// This is a fix for https://trello.com/c/1yExAPkV
	S3Clients map[string]*minio.Client
}

func NewContext() *Context {
	config := NewConfig()
	_logger := getLogger(config)
	return &Context{
		Config:         config,
		Logger:         _logger,
		NSQClient:      getNsqClient(config),
		RedisClient:    getRedisClient(config),
		RegistryClient: getRegistryClient(config, _logger),
		S3Clients:      getS3Clients(config, _logger),
	}
}

func getLogger(config *Config) *logging.Logger {
	logger, _ := logger.InitLogger(config.LogDir, config.LogLevel)
	return logger
}

func getNsqClient(config *Config) *network.NSQClient {
	return network.NewNSQClient(config.NsqURL)
}

func getRedisClient(config *Config) *network.RedisClient {
	return network.NewRedisClient(
		config.RedisURL,
		config.RedisPassword,
		config.RedisDefaultDB)
}

func getRegistryClient(config *Config, logger *logging.Logger) *network.RegistryClient {
	client, err := network.NewRegistryClient(
		config.RegistryURL,
		config.RegistryAPIVersion,
		config.RegistryAPIUser,
		config.RegistryAPIKey,
		logger)
	if err != nil {
		msg := fmt.Sprintf("Could not initialize Registry client: %v", err)
		panic(msg)
	}
	return client
}

func getS3Clients(config *Config, logger *logging.Logger) map[string]*minio.Client {
	s3Clients := make(map[string]*minio.Client, len(config.PreservationBuckets)+len(config.S3Credentials))
	useSSL := true
	if config.ConfigName == "dev" || config.ConfigName == "test" {
		useSSL = false // talking to localhost in dev and test
	}
	// Use NewWithOptions to force bucket lookup by path.
	// Note there's also credentials.NewStaticV2 for providers
	// who don't support V4.
	for provider, creds := range config.S3Credentials {
		client, err := minio.New(
			creds.Host,
			&minio.Options{
				Creds:  credentials.NewStaticV4(creds.KeyID, creds.SecretKey, ""),
				Secure: useSSL,
				// Increase max retries from default 10 to 20.
				// Part of issue https://trello.com/c/W8JXAdUO
				MaxRetries: 20,
			})
		if err != nil {
			panic(err)
		}
		s3Clients[provider] = client
		logger.Infof("Added general S3 client for provider %s", provider)

		// Add clients for specific preservation buckets, with region set explicitly.
		// https://trello.com/c/1yExAPkV
		for _, bucket := range config.PreservationBuckets {
			if bucket.Host == creds.Host {
				client, err := minio.New(
					creds.Host,
					&minio.Options{
						Creds:  credentials.NewStaticV4(creds.KeyID, creds.SecretKey, ""),
						Region: bucket.Region,
						Secure: useSSL,
					})
				if err != nil {
					panic(err)
				}
				s3Clients[bucket.Bucket] = client
				logger.Infof("Added bucket-specific S3 client for bucket %s in region %s at provider %s", bucket.Bucket, bucket.Region, provider)
			}
		}
	}
	return s3Clients
}

func (context *Context) S3StatObject(provider, bucket, key string) (minio.ObjectInfo, error) {
	emptyInfo := minio.ObjectInfo{}
	client := context.S3Clients[bucket]
	if client == nil {
		client = context.S3Clients[provider]
	}
	if client == nil {
		return emptyInfo, fmt.Errorf("No S3 client for provider %s or bucket %s", provider, bucket)
	}
	//client.TraceOn(GetTracer(context.Logger))
	info, err := client.StatObject(ctx.Background(), bucket, key, minio.StatObjectOptions{})
	//client.TraceOff()
	return info, err
}

func (context *Context) S3GetObject(provider, bucket, key string) (*minio.Object, error) {
	client := context.S3Clients[bucket]
	if client == nil {
		client = context.S3Clients[provider]
	}
	if client == nil {
		return nil, fmt.Errorf("No S3 client for provider %s or bucket %s", provider, bucket)
	}
	return client.GetObject(ctx.Background(), bucket, key, minio.GetObjectOptions{})
}
