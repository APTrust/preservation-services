package models

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
	NSQClient    network.NSQClientInterface
	PharosClient network.PharosClientInterface
	RedisClient  network.RedisClientInterface
	S3Clients    map[string]network.MinioClientInterface
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

func getNsqClient(config *Config) network.NSQClientInterface {
	return network.NewNSQClient(config.NsqURL)
}

func getRedisClient(config *Config) network.RedisClientInterface {
	return network.NewRedisClient(
		config.RedisURL,
		config.RedisPassword,
		config.RedisDefaultDB)
}

func getS3Clients(config *Config) map[string]network.MinioClientInterface {
	s3Clients := make(map[string]network.MinioClientInterface, len(config.S3Credentials))
	for name, creds := range config.S3Credentials {
		client, err := minio.New(
			creds.Host,
			creds.KeyId,
			creds.SecretKey,
			true) // true = use ssl
		if err != nil {
			panic(err)
		}
		s3Clients[name] = client
	}
	return s3Clients
}
