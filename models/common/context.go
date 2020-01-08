package common

import (
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
		PharosClient: nil, // doesn't exist yet
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

func getS3Clients(config *Config) map[string]*minio.Client {
	s3Clients := make(map[string]*minio.Client, len(config.S3Credentials))
	useSSL := true
	if config.ConfigName == "dev" || config.ConfigName == "test" {
		useSSL = false // talking to localhost in dev and test
	}
	for name, creds := range config.S3Credentials {
		client, err := minio.New(
			creds.Host,
			creds.KeyId,
			creds.SecretKey,
			useSSL)
		if err != nil {
			panic(err)
		}
		s3Clients[name] = client
	}
	return s3Clients
}
