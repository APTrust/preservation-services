package common

import (
	"fmt"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/testutil"
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
	ScriptDir               string
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
		LogLevel:                getLogLevel(v.GetString("LOG_LEVEL")),
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
		RedisRetries:            v.GetInt("REDIS_RETRIES"),
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
		ScriptDir:            v.GetString("SCRIPT_DIR"),
		StagingBucket:        v.GetString("STAGING_BUCKET"),
		StagingUploadRetries: v.GetInt("STAGING_UPLOAD_RETRIES"),
		StagingUploadRetryMs: v.GetDuration("STAGING_UPLOAD_RETRY_MS"),
		VolumeServiceURL:     v.GetString("VOLUME_SERVICE_URL"),
	}
}

func (config *Config) FormatIdentifierScript() string {
	return config.PathToScript("identify_format.sh")
}

func (config *Config) PathToScript(name string) string {
	return path.Join(config.ScriptDir, name)
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

func getLogLevel(level string) logging.Level {
	if level == "" {
		level = "INFO"
	}
	return logLevels[level]
}

// Expand ~ to home dir in path settings.
func (config *Config) expandPaths() {
	config.BaseWorkingDir = expandPath(config.BaseWorkingDir)
	config.IngestTempDir = expandPath(config.IngestTempDir)
	config.LogDir = expandPath(config.LogDir)
	config.RestoreDir = expandPath(config.RestoreDir)

	projectRoot := testutil.ProjectRoot()
	config.ScriptDir = strings.Replace(config.ScriptDir, "PROJECT_ROOT", projectRoot, 1)
}

func expandPath(dirName string) string {
	dir, err := util.ExpandTilde(dirName)
	if err != nil {
		panic(err)
	}
	return dir
}

func isLocalHost(host string) bool {
	return (strings.Contains(host, "localhost") ||
		strings.Contains(host, "127.0.0.1"))
}

func (config *Config) checkHostSafety() {
	if config.ConfigName == "dev" || config.ConfigName == "test" || runtime.GOOS == "darwin" {
		if !isLocalHost(config.NsqURL) {
			panic(fmt.Sprintf("Dev/Test setup cannot point to external NSQ instance %s", config.NsqURL))
		}
		if !isLocalHost(config.PharosURL) {
			panic(fmt.Sprintf("Dev/Test setup cannot point to external Pharos instance %s", config.PharosURL))
		}
		if !isLocalHost(config.RedisURL) {
			panic(fmt.Sprintf("Dev/Test setup cannot point to external Redis instance %s", config.RedisURL))
		}
		for _, name := range constants.S3Providers {
			if !isLocalHost(config.S3Credentials[name].Host) {
				panic(fmt.Sprintf("Dev/Test setup cannot point to external S3 URL %s for S3 service %s", config.S3Credentials[name].Host, name))
			}
		}
	}
}

func (config *Config) checkBasicSettings() {
	if config.BaseWorkingDir == "" {
		panic("Config is missing BaseWorkingDir")
	}
	if config.IngestTempDir == "" {
		panic("Config is missing IngestTempDir")
	}
	if config.LogDir == "" {
		panic("Config is missing LogDir")
	}
	if config.MaxDaysSinceFixityCheck == 0 {
		panic("Config is missing MaxDaysSinceFixityCheck")
	}
	if config.MaxFileSize == int64(0) {
		panic("Config is missing MaxFileSize")
	}
	if config.NsqLookupd == "" {
		panic("Config is missing NsqLookupd")
	}
	if config.NsqURL == "" {
		panic("Config is missing NsqURL")
	}
	if config.PharosAPIKey == "" {
		panic("Config is missing PharosAPIKey")
	}
	if config.PharosAPIUser == "" {
		panic("Config is missing PharosAPIUser")
	}
	if config.PharosAPIVersion == "" {
		panic("Config is missing PharosAPIVersion")
	}
	if config.PharosURL == "" {
		panic("Config is missing PharosURL")
	}
	if config.RedisDefaultDB < 0 || config.RedisDefaultDB > 16 {
		panic("RedisDefaultDB must be 0 <=> 16 (usually 0)")
	}
	// This one should be empty for dev/test
	// if c.RedisPassword == "" {
	// 	panic("Config is missing RedisPassword")
	// }
	if config.RedisRetries < 1 {
		panic("Config is missing RedisRetries")
	}
	if config.RedisRetryMs < time.Duration(1*time.Millisecond) {
		panic("Config is missing RedisRetryMs (be sure format is like 200ms)")
	}
	if config.RedisURL == "" {
		panic("Config is missing RedisURL")
	}
	// This one should be empty for dev/test
	// if c.RedisUser == "" {
	// 	panic("Config is missing RedisUser")
	// }
	if config.RestoreDir == "" {
		panic("Config is missing RestoreDir")
	}
	if config.StagingBucket == "" {
		panic("Config is missing StagingBucket")
	}
	if config.StagingUploadRetries < 1 {
		panic("Config is missing StagingUploadRetries")
	}
	if config.StagingUploadRetryMs < time.Duration(1*time.Millisecond) {
		panic("Config is missing StagingUploadRetryMs (be sure format is like 200ms)")
	}
	if config.VolumeServiceURL == "" {
		panic("Config is missing VolumeServiceURL")
	}
}

func (config *Config) checkS3Providers() {
	for _, name := range constants.S3Providers {
		provider := config.S3Credentials[name]
		if provider.Host == "" {
			panic(fmt.Sprintf("S3 provider %s is missing Host", name))
		}
		if provider.KeyId == "" {
			panic(fmt.Sprintf("S3 provider %s is missing KeyId", name))
		}
		if provider.SecretKey == "" {
			panic(fmt.Sprintf("S3 provider %s is missing SecretKey", name))
		}
	}
}

func (config *Config) sanityCheck() {
	// If this is dev or test env, don't let config point
	// to any external services. This prevents a dev/test
	// installation from touching data in demo and prod systems.
	config.checkBasicSettings()
	config.checkS3Providers()
	config.checkHostSafety()
}

func (config *Config) makeDirs() error {
	dirs := []string{
		config.IngestTempDir,
		config.LogDir,
		config.RestoreDir,
	}
	for _, dir := range dirs {
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			panic(err)
		}
	}
	return nil
}
