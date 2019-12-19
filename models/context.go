package models

import (
	"github.com/APTrust/preservation-services/network"
	"log"
)

// Will also need a Glacier client, since Minio does not support
// Glacier operations like restore. Since we need only 2 or 3
// calls, it may be better to write our own Glacier client than
// to work with that horrid AWS library.

type Context struct {
	Config       Config
	Logger       log.Logger
	NSQClient    network.NSQClientInterface
	PharosClient network.PharosClientInterface
	RedisClient  network.RedisClientInterface
	S3Client     network.MinioClientInterface
}
