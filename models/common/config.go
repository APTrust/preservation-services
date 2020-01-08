package common

import (
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/util"
	"github.com/op/go-logging"
	"os"
	"path"
	"runtime"
	"strings"
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

var ValidConfigs = []string{
	"demo",
	"dev",
	"production",
	"staging",
	"test",
}

// Returns a new config based on ENV var APT_SERVICES_CONFIG
func NewConfig() *Config {
	environment := os.Getenv("APT_SERVICES_CONFIG")
	if runtime.GOOS == "darwin" && (environment != "dev" && environment != "test") {
		panic("On Mac dev box, APT_SERVICES_CONFIG must be 'dev' or 'test'")
	}
	if !util.StringListContains(ValidConfigs, environment) {
		msg := fmt.Sprintf("No such environment: %s. Try APT_SERVICES_CONFIG=%s",
			environment, strings.Join(ValidConfigs, " | "))
		panic(msg)
	}
	config := newConfig(environment)
	config.ConfigName = environment
	return config
}

func newConfig(environment string) *Config {
	config := newDefaultConfig()
	config.addS3Credentials(environment)
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
		TestReceivingBucket:     "aptrust.poc.receiving",
		TestUnpackBucket:        "aptrust.poc.unpacked",
		TestPreservationBucket:  "aptrust.poc.preservation",
		VolumeServiceURL:        "http://localhost:8898",
	}
}

func (c *Config) addS3Credentials(environment string) {
	switch environment {
	case "dev", "test":
		c.S3Credentials = map[string]S3Credentials{
			constants.S3ClientAWS: S3Credentials{
				Host:      constants.TestMinioServerURL,
				KeyId:     constants.TestMinioUser,
				SecretKey: constants.TestMinioPwd,
			},
			constants.S3ClientWasabi: S3Credentials{
				Host:      constants.TestMinioServerURL,
				KeyId:     constants.TestMinioUser,
				SecretKey: constants.TestMinioPwd,
			},
		}
	case "staging", "demo", "prod":
		c.S3Credentials = map[string]S3Credentials{
			constants.S3ClientAWS: S3Credentials{
				Host:      "--TBD--",
				KeyId:     os.Getenv("AWS_ACCESS_KEY_ID"),
				SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
			},
			constants.S3ClientWasabi: S3Credentials{
				Host:      "--TBD--",
				KeyId:     os.Getenv("WASABI_ACCESS_KEY_ID"),
				SecretKey: os.Getenv("WASABI_SECRET_ACCESS_KEY"),
			},
		}
	default:
		panic(fmt.Sprintf("No such config: %s", environment))
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
