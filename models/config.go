package models

type Config struct {
	IngestStagingBucket string
	IngestTempDir       string
	LogFile             string
	PharosAPIKey        string
	PharosURL           string
	PharosUser          string
	RedisDefaultDB      int
	RedisPassword       string
	RedisURL            string
	RedisUser           string
	RestoreTempDir      string
	S3EndPoints         []S3EndPoint
}
