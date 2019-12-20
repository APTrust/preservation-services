package common

import (
	"fmt"
	"github.com/APTrust/preservation-services/util"
	"github.com/op/go-logging"
	"os"
	"path"
)

// TODO: Config should be modifiable on the fly, without having
// to restart app. Some config settings should probably be loaded
// in from Pharos. We should be able to update config settings in
// Pharos and have them applied on the fly.

// Log Levels:
//
// CRITICAL
// ERROR
// WARNING
// NOTICE
// INFO
// DEBUG

type Config struct {
	ConfigName              string
	GathererUploadRetries   int
	GathererUploadRetryMs   int
	IngestStagingBucket     string
	IngestTempDir           string
	LogDir                  string
	LogLevel                logging.Level
	MaxDaysSinceFixityCheck int
	MaxFileSize             int64
	NsqLookupd              string
	NsqURL                  string
	PharosAPIKey            string
	PharosAPIVersion        string
	PharosURL               string
	PharosUser              string
	RedisDefaultDB          int
	RedisPassword           string
	RedisRetries            int
	RedisRetryMs            int
	RedisURL                string
	RedisUser               string
	RestoreDir              string
	S3Credentials           map[string]S3Credentials
	TestReceivingBucket     string
	TestUnpackBucket        string
	TestPreservationBucket  string
	VolumeServiceURL        string
}

// Returns a new config based on ENV var APT_SERVICES_CONFIG
func NewConfig() *Config {
	environment := os.Getenv("APT_SERVICES_CONFIG")
	var config *Config
	switch environment {
	case "dev":
		config = newDevConfig()
	case "test":
		config = newTestConfig()
		// TODO: Consider automatically starting test S3 & Redis in this case
	default:
		panic(fmt.Sprintf("No such config: %s", environment))
	}
	config.ConfigName = environment
	return config
}

// Each env starts with default config and overrides items
// as necessary.
func newDevConfig() *Config {
	config := newDefaultConfig()
	// Customize here...
	config.makeDirs()
	return config
}

func newTestConfig() *Config {
	config := newDefaultConfig()
	// Customize here...
	config.makeDirs()
	return config
}

func newDefaultConfig() *Config {
	filesDir, err := util.ExpandTilde(path.Join("~", "tmp", "pres-serv"))
	// Config is necessary for the app to run, so we should just
	// die now if we can't determine basic info.
	if err != nil {
		panic(err)
	}
	return &Config{
		ConfigName:              "default",
		GathererUploadRetries:   3,
		GathererUploadRetryMs:   150,
		IngestStagingBucket:     "",
		IngestTempDir:           path.Join(filesDir, "ingest"),
		LogDir:                  path.Join(filesDir, "logs"),
		LogLevel:                logging.DEBUG,
		MaxDaysSinceFixityCheck: 90,
		MaxFileSize:             int64(5000000000),
		NsqLookupd:              "localhost:4161",
		NsqURL:                  "http://localhost:4151",
		PharosAPIKey:            os.Getenv("PHAROS_API_KEY"),
		PharosAPIVersion:        "v2",
		PharosURL:               "http://localhost:9292",
		PharosUser:              os.Getenv("PHAROS_API_USER"),
		RedisDefaultDB:          0,
		RedisPassword:           "",
		RedisRetries:            3,
		RedisRetryMs:            150,
		RedisURL:                "localhost:6379",
		RedisUser:               "",
		RestoreDir:              path.Join(filesDir, "restore"),
		S3Credentials: map[string]S3Credentials{
			"AWS": S3Credentials{
				Host:      "localhost",
				KeyId:     os.Getenv("AWS_ACCESS_KEY_ID"),
				SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
			},
			"Wasabi": S3Credentials{
				Host:      "localhost",
				KeyId:     os.Getenv("WASABI_ACCESS_KEY_ID"),
				SecretKey: os.Getenv("WASABI_SECRET_ACCESS_KEY"),
			},
		},
		TestReceivingBucket:    "aptrust.poc.receiving",
		TestUnpackBucket:       "aptrust.poc.unpacked",
		TestPreservationBucket: "aptrust.poc.preservation",
		VolumeServiceURL:       "http://localhost:8898",
	}
}

func (c *Config) makeDirs() error {
	dirs := []string{
		c.IngestTempDir,
		c.LogDir,
		c.RestoreDir,
	}
	for _, dir := range dirs {
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			return err
		}
	}
	return nil
}
