package common

import (
	"fmt"

	"github.com/APTrust/preservation-services/network"
	"github.com/APTrust/preservation-services/util/logger"
	"github.com/minio/minio-go/v6"
	"github.com/op/go-logging"
)

// Will also need a Glacier client, since Minio does not support
// Glacier operations like restore. Since we need only 2 or 3
// calls, it may be better to write our own Glacier client than
// to work with that horrid AWS library.

type Context struct {
	Config       *Config
	Logger       *logging.Logger
	NSQClient    *network.NSQClient
	PharosClient *network.PharosClient
	RedisClient  *network.RedisClient
	S3Clients    map[string]*minio.Client
}

func NewContext() *Context {
	config := NewConfig()
	return &Context{
		Config:       config,
		Logger:       getLogger(config),
		NSQClient:    getNsqClient(config),
		PharosClient: getPharosClient(config),
		RedisClient:  getRedisClient(config),
		S3Clients:    getS3Clients(config),
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

func getPharosClient(config *Config) *network.PharosClient {
	client, err := network.NewPharosClient(
		config.PharosURL,
		config.PharosAPIVersion,
		config.PharosAPIUser,
		config.PharosAPIKey)
	if err != nil {
		msg := fmt.Sprintf("Could not initialize Pharos client: %v", err)
		panic(msg)
	}
	return client
}

func getS3Clients(config *Config) map[string]*minio.Client {
	s3Clients := make(map[string]*minio.Client, len(config.S3Credentials))
	useSSL := true
	if config.ConfigName == "dev" || config.ConfigName == "test" {
		useSSL = false // talking to localhost in dev and test
	}
	for _, target := range config.UploadTargets {
		creds := config.CredentialsForS3Host(target.Host)
		if creds == nil {
			panic(fmt.Sprintf("Missing credentials for S3 host %s", target.Host))
		}
		client, err := minio.NewWithRegion(
			creds.Host,
			creds.KeyID,
			creds.SecretKey,
			useSSL,
			target.Region)
		if err != nil {
			panic(err)
		}
		s3Clients[target.Provider] = client
	}
	return s3Clients
}

func (context *Context) S3StatObject(provider, bucket, key string) (minio.ObjectInfo, error) {
	emptyInfo := minio.ObjectInfo{}
	client := context.S3Clients[provider]
	if client == nil {
		return emptyInfo, fmt.Errorf("No S3 client for provider %s", provider)
	}
	return client.StatObject(bucket, key, minio.StatObjectOptions{})
}

func (context *Context) S3GetObject(provider, bucket, key string) (*minio.Object, error) {
	client := context.S3Clients[provider]
	if client == nil {
		return nil, fmt.Errorf("No S3 client for provider %s", provider)
	}
	return client.GetObject(bucket, key, minio.GetObjectOptions{})
}
