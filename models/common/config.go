package common

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/util"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

type Config struct {
	BaseWorkingDir          string
	BucketStandardOR        string
	BucketStandardVA        string
	BucketGlacierOH         string
	BucketGlacierOR         string
	BucketGlacierVA         string
	BucketGlacierDeepOH     string
	BucketGlacierDeepOR     string
	BucketGlacierDeepVA     string
	BucketWasabiOR          string
	BucketWasabiVA          string
	ConfigName              string
	IngestTempDir           string
	LogDir                  string
	LogLevel                logging.Level
	MaxDaysSinceFixityCheck int
	MaxFileSize             int64
	MaxWorkerAttempts       int
	NsqLookupd              string
	NsqURL                  string
	PharosAPIKey            string `json:"-"`
	PharosAPIUser           string `json:"-"`
	PharosAPIVersion        string
	PharosURL               string
	ProfilesDir             string
	RedisDefaultDB          int
	RedisPassword           string `json:"-"`
	RedisRetries            int
	RedisRetryMs            time.Duration
	RedisURL                string
	RedisUser               string `json:"-"`
	RestoreDir              string
	S3AWSHost               string
	S3Credentials           map[string]S3Credentials `json:"-"`
	S3LocalHost             string
	S3WasabiHost            string
	StagingBucket           string
	StagingUploadRetries    int
	StagingUploadRetryMs    time.Duration
	UploadTargets           []*UploadTarget
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

// Returns a new config based on ENV var APT_ENV
func NewConfig() *Config {
	config := loadConfig()
	config.expandPaths()
	config.initUploadTargets()
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
		util.PrintAndExit(fmt.Sprintf("Fatal error config file: %v \n", err))
	}
	return &Config{
		BaseWorkingDir:          v.GetString("BASE_WORKING_DIR"),
		BucketStandardOR:        v.GetString("BUCKET_STANDARD_OR"),
		BucketStandardVA:        v.GetString("BUCKET_STANDARD_VA"),
		BucketGlacierOH:         v.GetString("BUCKET_GLACIER_OH"),
		BucketGlacierOR:         v.GetString("BUCKET_GLACIER_OR"),
		BucketGlacierVA:         v.GetString("BUCKET_GLACIER_VA"),
		BucketGlacierDeepOH:     v.GetString("BUCKET_GLACIER_DEEP_OH"),
		BucketGlacierDeepOR:     v.GetString("BUCKET_GLACIER_DEEP_OR"),
		BucketGlacierDeepVA:     v.GetString("BUCKET_GLACIER_DEEP_VA"),
		BucketWasabiOR:          v.GetString("BUCKET_WASABI_OR"),
		BucketWasabiVA:          v.GetString("BUCKET_WASABI_VA"),
		ConfigName:              envName,
		IngestTempDir:           v.GetString("INGEST_TEMP_DIR"),
		LogDir:                  v.GetString("LOG_DIR"),
		LogLevel:                getLogLevel(v.GetString("LOG_LEVEL")),
		MaxDaysSinceFixityCheck: v.GetInt("MAX_DAYS_SINCE_LAST_FIXITY"),
		MaxFileSize:             v.GetInt64("MAX_FILE_SIZE"),
		MaxWorkerAttempts:       v.GetInt("MAX_WORKER_ATTEMPTS"),
		NsqLookupd:              v.GetString("NSQ_LOOKUPD"),
		NsqURL:                  v.GetString("NSQ_URL"),
		PharosAPIKey:            v.GetString("PHAROS_API_KEY"),
		PharosAPIUser:           v.GetString("PHAROS_API_USER"),
		PharosAPIVersion:        v.GetString("PHAROS_API_VERSION"),
		PharosURL:               v.GetString("PHAROS_URL"),
		ProfilesDir:             v.GetString("PROFILES_DIR"),
		RedisDefaultDB:          v.GetInt("REDIS_DEFAULT_DB"),
		RedisPassword:           v.GetString("REDIS_PASSWORD"),
		RedisRetries:            v.GetInt("REDIS_RETRIES"),
		RedisRetryMs:            v.GetDuration("REDIS_RETRY_MS"),
		RedisURL:                v.GetString("REDIS_URL"),
		RedisUser:               v.GetString("REDIS_USER"),
		RestoreDir:              v.GetString("RESTORE_DIR"),
		S3AWSHost:               v.GetString("S3_AWS_HOST"),
		S3Credentials: map[string]S3Credentials{
			constants.StorageProviderAWS: S3Credentials{
				Host:      v.GetString("S3_AWS_HOST"),
				KeyID:     v.GetString("S3_AWS_KEY"),
				SecretKey: v.GetString("S3_AWS_SECRET"),
			},
			constants.StorageProviderLocal: S3Credentials{
				Host:      v.GetString("S3_LOCAL_HOST"),
				KeyID:     v.GetString("S3_LOCAL_KEY"),
				SecretKey: v.GetString("S3_LOCAL_SECRET"),
			},
			constants.StorageProviderWasabi: S3Credentials{
				Host:      v.GetString("S3_WASABI_HOST"),
				KeyID:     v.GetString("S3_WASABI_KEY"),
				SecretKey: v.GetString("S3_WASABI_SECRET"),
			},
		},
		S3LocalHost:          v.GetString("S3_LOCAL_HOST"),
		S3WasabiHost:         v.GetString("S3_WASABI_HOST"),
		StagingBucket:        v.GetString("STAGING_BUCKET"),
		StagingUploadRetries: v.GetInt("STAGING_UPLOAD_RETRIES"),
		StagingUploadRetryMs: v.GetDuration("STAGING_UPLOAD_RETRY_MS"),
		VolumeServiceURL:     v.GetString("VOLUME_SERVICE_URL"),
	}
}

// UploadTargetFor returns the upload targets for the specified storage option.
// Storage options are enumerated in constants.StorageOptions. For most options,
// this will return a single item. For the Standard storage option, it returns
// two upload targets.
func (config *Config) UploadTargetsFor(storageOption string) []*UploadTarget {
	targets := make([]*UploadTarget, 0)
	for _, target := range config.UploadTargets {
		if target.OptionName == storageOption {
			targets = append(targets, target)
		}
	}
	return targets
}

func getEnvVars() (string, string) {
	cwd, _ := os.Getwd()
	configDir := getRequiredEnvVar("APT_CONFIG_DIR", cwd)
	envName := getRequiredEnvVar("APT_ENV", "")
	return configDir, envName
}

func getRequiredEnvVar(varName, defaultValue string) string {
	value := os.Getenv(varName)
	if value == "" {
		value = defaultValue
	}
	if value == "" {
		util.PrintAndExit(fmt.Sprintf("Required env var %s not set", varName))
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
	config.ProfilesDir = expandPath(config.ProfilesDir)
	config.RestoreDir = expandPath(config.RestoreDir)
}

func expandPath(dirName string) string {
	dir, err := util.ExpandTilde(dirName)
	if err != nil {
		util.PrintAndExit(err.Error())
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
			util.PrintAndExit(fmt.Sprintf("Dev/Test setup cannot point to external NSQ instance %s", config.NsqURL))
		}
		if !isLocalHost(config.PharosURL) {
			util.PrintAndExit(fmt.Sprintf("Dev/Test setup cannot point to external Pharos instance %s", config.PharosURL))
		}
		if !isLocalHost(config.RedisURL) {
			util.PrintAndExit(fmt.Sprintf("Dev/Test setup cannot point to external Redis instance %s", config.RedisURL))
		}
		for _, name := range constants.StorageProviders {
			if !isLocalHost(config.S3Credentials[name].Host) {
				util.PrintAndExit(fmt.Sprintf("Dev/Test setup cannot point to external S3 URL %s for S3 service %s", config.S3Credentials[name].Host, name))
			}
		}
	}
}

func (config *Config) checkBasicSettings() {
	if config.BaseWorkingDir == "" {
		util.PrintAndExit("Config is missing BaseWorkingDir")
	}
	if config.BucketStandardOR == "" {
		util.PrintAndExit("Config is missing BucketStandardOR")
	}
	if config.BucketStandardVA == "" {
		util.PrintAndExit("Config is missing BucketStandardVA")
	}
	if config.BucketGlacierOH == "" {
		util.PrintAndExit("Config is missing BucketGlacierOH")
	}
	if config.BucketGlacierOR == "" {
		util.PrintAndExit("Config is missing BucketGlacierOR")
	}
	if config.BucketGlacierVA == "" {
		util.PrintAndExit("Config is missing BucketGlacierVA")
	}
	if config.BucketGlacierDeepOH == "" {
		util.PrintAndExit("Config is missing BucketGlacierDeepOH")
	}
	if config.BucketGlacierDeepOR == "" {
		util.PrintAndExit("Config is missing BucketGlacierDeepOR")
	}
	if config.BucketGlacierDeepVA == "" {
		util.PrintAndExit("Config is missing BucketGlacierDeepVA")
	}
	if config.BucketWasabiOR == "" {
		util.PrintAndExit("Config is missing BucketWasabiOR")
	}
	if config.BucketWasabiVA == "" {
		util.PrintAndExit("Config is missing BucketWasabiVA")
	}
	if config.IngestTempDir == "" {
		util.PrintAndExit("Config is missing IngestTempDir")
	}
	if config.LogDir == "" {
		util.PrintAndExit("Config is missing LogDir")
	}
	if config.MaxDaysSinceFixityCheck == 0 {
		util.PrintAndExit("Config is missing MaxDaysSinceFixityCheck")
	}
	if config.MaxFileSize == int64(0) {
		util.PrintAndExit("Config is missing MaxFileSize")
	}
	if config.NsqLookupd == "" {
		util.PrintAndExit("Config is missing NsqLookupd")
	}
	if config.NsqURL == "" {
		util.PrintAndExit("Config is missing NsqURL")
	}
	if config.PharosAPIKey == "" {
		util.PrintAndExit("Config is missing PharosAPIKey")
	}
	if config.PharosAPIUser == "" {
		util.PrintAndExit("Config is missing PharosAPIUser")
	}
	if config.PharosAPIVersion == "" {
		util.PrintAndExit("Config is missing PharosAPIVersion")
	}
	if config.PharosURL == "" {
		util.PrintAndExit("Config is missing PharosURL")
	}
	if config.ProfilesDir == "" {
		util.PrintAndExit("Config is missing ProfilesDir")
	}
	if config.RedisDefaultDB < 0 || config.RedisDefaultDB > 16 {
		util.PrintAndExit("RedisDefaultDB must be 0 <=> 16 (usually 0)")
	}
	// This one should be empty for dev/test
	// if c.RedisPassword == "" {
	// 	util.PrintAndExit("Config is missing RedisPassword")
	// }
	if config.RedisRetries < 1 {
		util.PrintAndExit("Config is missing RedisRetries")
	}
	if config.RedisRetryMs < time.Duration(1*time.Millisecond) {
		util.PrintAndExit("Config is missing RedisRetryMs (be sure format is like 200ms)")
	}
	if config.RedisURL == "" {
		util.PrintAndExit("Config is missing RedisURL")
	}
	// This one should be empty for dev/test
	// if c.RedisUser == "" {
	// 	util.PrintAndExit("Config is missing RedisUser")
	// }
	if config.RestoreDir == "" {
		util.PrintAndExit("Config is missing RestoreDir")
	}
	if config.StagingBucket == "" {
		util.PrintAndExit("Config is missing StagingBucket")
	}
	if config.StagingUploadRetries < 1 {
		util.PrintAndExit("Config is missing StagingUploadRetries")
	}
	if config.StagingUploadRetryMs < time.Duration(1*time.Millisecond) {
		util.PrintAndExit("Config is missing StagingUploadRetryMs (be sure format is like 200ms)")
	}
	if config.VolumeServiceURL == "" {
		util.PrintAndExit("Config is missing VolumeServiceURL")
	}
}

func (config *Config) checkS3Providers() {
	for _, name := range constants.StorageProviders {
		provider := config.S3Credentials[name]
		if provider.Host == "" {
			util.PrintAndExit(fmt.Sprintf("S3 provider %s is missing Host", name))
		}
		if provider.KeyID == "" {
			util.PrintAndExit(fmt.Sprintf("S3 provider %s is missing KeyId", name))
		}
		if provider.SecretKey == "" {
			util.PrintAndExit(fmt.Sprintf("S3 provider %s is missing SecretKey", name))
		}
	}
}

func (config *Config) checkUploadTargets() {
	for _, target := range config.UploadTargets {
		if target.Host == "" {
			util.PrintAndExit(fmt.Sprintf("S3 target %s is missing Host", target.OptionName))
		}
		if target.Bucket == "" {
			util.PrintAndExit(fmt.Sprintf("S3 provider %s is missing Bucket", target.OptionName))
		}
		if target.Region == "" {
			util.PrintAndExit(fmt.Sprintf("S3 provider %s is missing Region", target.OptionName))
		}
	}
}

func (config *Config) sanityCheck() {
	// If this is dev or test env, don't let config point
	// to any external services. This prevents a dev/test
	// installation from touching data in demo and prod systems.
	config.checkBasicSettings()
	config.checkS3Providers()
	config.checkUploadTargets()
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
			util.PrintAndExit(err.Error())
		}
	}
	return nil
}

func (config *Config) initUploadTargets() {
	config.UploadTargets = []*UploadTarget{
		&UploadTarget{
			Bucket:       config.BucketStandardVA,
			Description:  "AWS Virginia S3 bucket for Standard primary preservation",
			Host:         config.S3AWSHost,
			OptionName:   constants.StorageStandard,
			Provider:     constants.StorageProviderAWS,
			Region:       constants.RegionAWSUSEast1,
			StorageClass: constants.StorageClassStandard,
		},
		&UploadTarget{
			Bucket:       config.BucketStandardOR,
			Description:  "AWS Oregon Glacier bucket for Standard storage repilication",
			Host:         config.S3AWSHost,
			OptionName:   constants.StorageStandard,
			Provider:     constants.StorageProviderAWS,
			Region:       constants.RegionAWSUSWest2,
			StorageClass: constants.StorageClassGlacier,
		},
		&UploadTarget{
			Bucket:       config.BucketGlacierOH,
			Description:  "AWS Ohio Glacier storage",
			Host:         config.S3AWSHost,
			OptionName:   constants.StorageGlacierOH,
			Provider:     constants.StorageProviderAWS,
			Region:       constants.RegionAWSUSEast2,
			StorageClass: constants.StorageClassGlacier,
		},
		&UploadTarget{
			Bucket:       config.BucketGlacierOR,
			Description:  "AWS Oregon Glacier storage",
			Host:         config.S3AWSHost,
			OptionName:   constants.StorageGlacierOR,
			Provider:     constants.StorageProviderAWS,
			Region:       constants.RegionAWSUSWest2,
			StorageClass: constants.StorageClassGlacier,
		},
		&UploadTarget{
			Bucket:       config.BucketGlacierVA,
			Description:  "AWS Virginia Glacier storage",
			Host:         config.S3AWSHost,
			OptionName:   constants.StorageGlacierVA,
			Provider:     constants.StorageProviderAWS,
			Region:       constants.RegionAWSUSEast1,
			StorageClass: constants.StorageClassGlacier,
		},
		&UploadTarget{
			Bucket:       config.BucketGlacierDeepOH,
			Description:  "AWS Ohio Glacier deep storage",
			Host:         config.S3AWSHost,
			OptionName:   constants.StorageGlacierDeepOH,
			Provider:     constants.StorageProviderAWS,
			Region:       constants.RegionAWSUSEast2,
			StorageClass: constants.StorageClassGlacierDeep,
		},
		&UploadTarget{
			Bucket:       config.BucketGlacierDeepOR,
			Description:  "AWS Oregon Glacier deep storage",
			Host:         config.S3AWSHost,
			OptionName:   constants.StorageGlacierDeepOR,
			Provider:     constants.StorageProviderAWS,
			Region:       constants.RegionAWSUSWest2,
			StorageClass: constants.StorageClassGlacierDeep,
		},
		&UploadTarget{
			Bucket:       config.BucketGlacierDeepVA,
			Description:  "AWS Virginia Glacier deep storage",
			Host:         config.S3AWSHost,
			OptionName:   constants.StorageGlacierDeepVA,
			Provider:     constants.StorageProviderAWS,
			Region:       constants.RegionAWSUSEast1,
			StorageClass: constants.StorageClassGlacierDeep,
		},
		&UploadTarget{
			Bucket:       config.BucketWasabiOR,
			Description:  "Wasabi Oregon storage",
			Host:         config.S3WasabiHost,
			OptionName:   constants.StorageWasabiOR,
			Provider:     constants.StorageProviderWasabi,
			Region:       constants.RegionWasabiUSWest1,
			StorageClass: constants.StorageClassWasabi,
		},
		&UploadTarget{
			Bucket:       config.BucketWasabiVA,
			Description:  "Wasabi Virginia storage (us-east-1)",
			Host:         config.S3WasabiHost,
			OptionName:   constants.StorageWasabiVA,
			Provider:     constants.StorageProviderWasabi,
			Region:       constants.RegionWasabiUSEast1,
			StorageClass: constants.StorageClassWasabi,
		},
	}
}

// ToJSON serializes the config to JSON for logging purposes.
// It omits some sensitive data, such as the Pharos API key and
// AWS credentials.
func (config *Config) ToJSON() string {
	data, _ := json.Marshal(config)
	return string(data)
}
