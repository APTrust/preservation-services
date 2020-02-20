package common

import (
	"fmt"
	"os"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/util"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

type Config struct {
	BaseWorkingDir          string
	ConfigName              string
	IngestTempDir           string
	LogDir                  string
	LogLevel                logging.Level
	MaxDaysSinceFixityCheck int
	MaxFileSize             int64
	NsqLookupd              string
	NsqURL                  string
	PharosAPIKey            string
	PharosAPIUser           string
	PharosAPIVersion        string
	PharosURL               string
	RedisDefaultDB          int
	RedisPassword           string
	RedisRetries            int
	RedisRetryMs            time.Duration
	RedisURL                string
	RedisUser               string
	RestoreDir              string
	S3Credentials           map[string]S3Credentials
	StagingBucket           string
	StagingUploadRetries    int
	StagingUploadRetryMs    time.Duration
	VolumeServiceURL        string
}

var logLevels = map[string]logging.Level{
	"CRITICAL": logging.CRITICAL,
	"ERROR":    logging.ERROR,
	"WARNING":  logging.WARNING,
	"NOTICE":   logging.NOTICE,
	"INFO":     logging.INFO,
	"DEBUG":    logging.DEBUG,
}

// Returns a new config based on ENV var APT_SERVICES_CONFIG
func NewConfig() *Config {
	config := loadConfig()
	config.expandPaths()
	config.sanityCheck()
	config.makeDirs()
	return config
}

func loadConfig() *Config {
	configDir, envName := getEnvVars()
	v := viper.New()
	v.AddConfigPath(configDir)
	v.SetConfigName(".env." + envName)
	v.SetConfigType("env")
	err := v.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	return &Config{
		BaseWorkingDir:          v.GetString("BASE_WORKING_DIR"),
		ConfigName:              envName,
		IngestTempDir:           v.GetString("INGEST_TEMP_DIR"),
		LogDir:                  v.GetString("LOG_DIR"),
		LogLevel:                logLevels[v.GetString("LOG_LEVEL")],
		MaxDaysSinceFixityCheck: v.GetInt("MAX_DAYS_SINCE_LAST_FIXITY"),
		MaxFileSize:             v.GetInt64("MAX_FILE_SIZE"),
		NsqLookupd:              v.GetString("NSQ_LOOKUPD"),
		NsqURL:                  v.GetString("NSQ_URL"),
		PharosAPIKey:            v.GetString("PHAROS_API_KEY"),
		PharosAPIUser:           v.GetString("PHAROS_API_USER"),
		PharosAPIVersion:        v.GetString("PHAROS_API_VERSION"),
		PharosURL:               v.GetString("PHAROS_URL"),
		RedisDefaultDB:          v.GetInt("REDIS_DEFAULT_DB"),
		RedisPassword:           v.GetString("REDIS_PASSWORD"),
		RedisRetries:            v.GetInt("PHAROS_RETRIES"),
		RedisRetryMs:            v.GetDuration("REDIS_RETRY_MS"),
		RedisURL:                v.GetString("REDIS_URL"),
		RedisUser:               v.GetString("REDIS_USER"),
		RestoreDir:              v.GetString("RESTORE_DIR"),
		S3Credentials: map[string]S3Credentials{
			constants.S3ClientAWS: S3Credentials{
				Host:      v.GetString("S3_AWS_HOST"),
				KeyId:     v.GetString("S3_AWS_KEY"),
				SecretKey: v.GetString("S3_AWS_SECRET"),
			},
			constants.S3ClientLocal: S3Credentials{
				Host:      v.GetString("S3_LOCAL_HOST"),
				KeyId:     v.GetString("S3_LOCAL_KEY"),
				SecretKey: v.GetString("S3_LOCAL_SECRET"),
			},
			constants.S3ClientWasabi: S3Credentials{
				Host:      v.GetString("S3_WASABI_HOST"),
				KeyId:     v.GetString("S3_WASABI_KEY"),
				SecretKey: v.GetString("S3_WASABI_SECRET"),
			},
		},
		StagingBucket:        v.GetString("STAGING_BUCKET"),
		StagingUploadRetries: v.GetInt("STAGING_UPLOAD_RETRIES"),
		StagingUploadRetryMs: v.GetDuration("STAGING_UPLOAD_RETRY_MS"),
		VolumeServiceURL:     v.GetString("VOLUME_SERVICE_URL"),
	}
}

func getEnvVars() (string, string) {
	configDir := getRequiredEnvVar("APT_CONFIG_DIR")
	envName := getRequiredEnvVar("APT_SERVICES_CONFIG")
	return configDir, envName
}

func getRequiredEnvVar(varName string) string {
	value := os.Getenv(varName)
	if value == "" {
		panic(fmt.Sprintf("Required env var %s not set", varName))
	}
	return value
}

// Expand ~ to home dir in path settings.
func (c *Config) expandPaths() {
	c.BaseWorkingDir = expandPath(c.BaseWorkingDir)
	c.IngestTempDir = expandPath(c.IngestTempDir)
	c.LogDir = expandPath(c.LogDir)
	c.RestoreDir = expandPath(c.RestoreDir)
}

func expandPath(dirName string) string {
	dir, err := util.ExpandTilde(dirName)
	if err != nil {
		panic(err)
	}
	return dir
}

func (c *Config) sanityCheck() {
	// If this is dev or test env, don't let config point
	// to any external services. This prevents a dev/test
	// installation from touching data in demo and prod systems.

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
			panic(err)
		}
	}
	return nil
}
