package common

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/util"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

type Config struct {
	APTDeleteBufferSize                   int
	APTDeleteMaxAttempts                  int
	APTDeleteWorkers                      int
	APTFixityBufferSize                   int
	APTFixityMaxAttempts                  int
	APTFixityWorkers                      int
	APTQueueInterval                      time.Duration
	BagRestorerBufferSize                 int
	BagRestorerMaxAttempts                int
	BagRestorerWorkers                    int
	BaseWorkingDir                        string
	BucketGlacierDeepOH                   string
	BucketGlacierDeepOR                   string
	BucketGlacierDeepVA                   string
	BucketGlacierOH                       string
	BucketGlacierOR                       string
	BucketGlacierVA                       string
	BucketStandardOR                      string
	BucketStandardVA                      string
	BucketWasabiOR                        string
	BucketWasabiVA                        string
	ConfigFilePath                        string
	ConfigName                            string
	FileRestorerBufferSize                int
	FileRestorerMaxAttempts               int
	FileRestorerWorkers                   int
	GlacierRestorerBufferSize             int
	GlacierRestorerMaxAttempts            int
	GlacierRestorerWorkers                int
	IngestBucketReaderInterval            time.Duration
	IngestCleanupBufferSize               int
	IngestCleanupMaxAttempts              int
	IngestCleanupWorkers                  int
	IngestFormatIdentifierBufferSize      int
	IngestFormatIdentifierMaxAttempts     int
	IngestFormatIdentifierWorkers         int
	IngestPreFetchBufferSize              int
	IngestPreFetchMaxAttempts             int
	IngestPreFetchWorkers                 int
	IngestPreservationUploaderBufferSize  int
	IngestPreservationUploaderMaxAttempts int
	IngestPreservationUploaderWorkers     int
	IngestPreservationVerifierBufferSize  int
	IngestPreservationVerifierMaxAttempts int
	IngestPreservationVerifierWorkers     int
	IngestRecorderBufferSize              int
	IngestRecorderMaxAttempts             int
	IngestRecorderWorkers                 int
	IngestStagingUploaderBufferSize       int
	IngestStagingUploaderMaxAttempts      int
	IngestStagingUploaderWorkers          int
	IngestTempDir                         string
	IngestValidatorBufferSize             int
	IngestValidatorMaxAttempts            int
	IngestValidatorWorkers                int
	LogDir                                string
	LogLevel                              logging.Level
	MaxDaysSinceFixityCheck               int
	MaxFileSize                           int64
	MaxFixityItemsPerRun                  int
	MaxWorkerAttempts                     int
	NsqLookupd                            string
	NsqURL                                string
	PharosAPIKey                          string `json:"-"`
	PharosAPIUser                         string `json:"-"`
	PharosAPIVersion                      string
	PharosURL                             string
	PreservationBuckets                   []*PreservationBucket
	ProfilesDir                           string
	QueueFixityInterval                   time.Duration
	RedisDefaultDB                        int
	RedisPassword                         string `json:"-"`
	RedisRetries                          int
	RedisRetryMs                          time.Duration
	RedisURL                              string
	RedisUser                             string `json:"-"`
	ReingestManagerBufferSize             int
	ReingestManagerMaxAttempts            int
	ReingestManagerWorkers                int
	RestoreDir                            string
	S3AWSHost                             string
	S3Credentials                         map[string]*S3Credentials `json:"-"`
	S3LocalHost                           string
	S3WasabiHostOR                        string
	S3WasabiHostVA                        string
	StagingBucket                         string
	StagingUploadRetries                  int
	StagingUploadRetryMs                  time.Duration
	VolumeServiceURL                      string
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
	config.initPreservationBuckets()
	config.sanityCheck()
	config.makeDirs()
	return config
}

// This returns the default config directory and file.
// In most cases, that will be the .env file in the
// current working directory. When running automated tests,
// however, go changes into the subdirectories that contain
// the test files, so this resolves configDir to the project
// root directory.
func configDirAndFile() (configDir string, configFile string) {
	configDir, _ = os.Getwd()
	envName := os.Getenv("APT_ENV")
	configFile = ".env"
	if envName != "" {
		configFile = ".env." + envName
	}
	if util.TestsAreRunning() {
		configDir = util.ProjectRoot()
	}
	return configDir, configFile
}

func loadConfig() *Config {
	configDir, configFile := configDirAndFile()
	v := viper.New()
	v.AddConfigPath(configDir)
	v.SetConfigName(configFile)
	v.SetConfigType("env")
	err := v.ReadInConfig()
	if err != nil {
		util.PrintAndExit(fmt.Sprintf("Fatal error config file: %v \n", err))
	}
	return &Config{
		APTQueueInterval:                      v.GetDuration("APT_QUEUE_INTERVAL"),
		BaseWorkingDir:                        v.GetString("BASE_WORKING_DIR"),
		BucketGlacierDeepOH:                   v.GetString("BUCKET_GLACIER_DEEP_OH"),
		BucketGlacierDeepOR:                   v.GetString("BUCKET_GLACIER_DEEP_OR"),
		BucketGlacierDeepVA:                   v.GetString("BUCKET_GLACIER_DEEP_VA"),
		BucketGlacierOH:                       v.GetString("BUCKET_GLACIER_OH"),
		BucketGlacierOR:                       v.GetString("BUCKET_GLACIER_OR"),
		BucketGlacierVA:                       v.GetString("BUCKET_GLACIER_VA"),
		BucketStandardOR:                      v.GetString("BUCKET_STANDARD_OR"),
		BucketStandardVA:                      v.GetString("BUCKET_STANDARD_VA"),
		BucketWasabiOR:                        v.GetString("BUCKET_WASABI_OR"),
		BucketWasabiVA:                        v.GetString("BUCKET_WASABI_VA"),
		ConfigFilePath:                        path.Join(configDir, configFile),
		ConfigName:                            strings.Replace(configFile, ".env.", "", 1),
		GlacierRestorerMaxAttempts:            v.GetInt("GLACIER_RESTORER_MAX_ATTEMPTS"),
		GlacierRestorerWorkers:                v.GetInt("GLACIER_RESTORER_WORKERS"),
		IngestBucketReaderInterval:            v.GetDuration("INGEST_BUCKET_READER_INTERVAL"),
		IngestCleanupBufferSize:               v.GetInt("INGEST_CLEANUP_BUFFER_SIZE"),
		IngestCleanupMaxAttempts:              v.GetInt("INGEST_CLEANUP_MAX_ATTEMPTS"),
		IngestCleanupWorkers:                  v.GetInt("INGEST_CLEANUP_WORKERS"),
		IngestFormatIdentifierBufferSize:      v.GetInt("INGEST_FORMAT_IDENTIFIER_BUFFER_SIZE"),
		IngestFormatIdentifierMaxAttempts:     v.GetInt("INGEST_FORMAT_IDENTIFIER_MAX_ATTEMPTS"),
		IngestFormatIdentifierWorkers:         v.GetInt("INGEST_FORMAT_IDENTIFIER_WORKERS"),
		IngestPreFetchBufferSize:              v.GetInt("INGEST_PRE_FETCH_BUFFER_SIZE"),
		IngestPreFetchMaxAttempts:             v.GetInt("INGEST_PRE_FETCH_MAX_ATTEMPTS"),
		IngestPreFetchWorkers:                 v.GetInt("INGEST_PRE_FETCH_WORKERS"),
		IngestPreservationUploaderBufferSize:  v.GetInt("INGEST_PRESERVATION_UPLOADER_BUFFER_SIZE"),
		IngestPreservationUploaderMaxAttempts: v.GetInt("INGEST_PRESERVATION_UPLOADER_MAX_ATTEMPTS"),
		IngestPreservationUploaderWorkers:     v.GetInt("INGEST_PRESERVATION_UPLOADER_WORKERS"),
		IngestPreservationVerifierBufferSize:  v.GetInt("INGEST_PRESERVATION_VERIFIER_BUFFER_SIZE"),
		IngestPreservationVerifierMaxAttempts: v.GetInt("INGEST_PRESERVATION_VERIFIER_MAX_ATTEMPTS"),
		IngestPreservationVerifierWorkers:     v.GetInt("INGEST_PRESERVATION_VERIFIER_WORKERS"),
		IngestRecorderBufferSize:              v.GetInt("INGEST_RECORDER_BUFFER_SIZE"),
		IngestRecorderMaxAttempts:             v.GetInt("INGEST_RECORDER_MAX_ATTEMPTS"),
		IngestRecorderWorkers:                 v.GetInt("INGEST_RECORDER_WORKERS"),
		IngestStagingUploaderBufferSize:       v.GetInt("INGEST_STAGING_UPLOADER_BUFFER_SIZE"),
		IngestStagingUploaderMaxAttempts:      v.GetInt("INGEST_STAGING_UPLOADER_MAX_ATTEMPTS"),
		IngestStagingUploaderWorkers:          v.GetInt("INGEST_STAGING_UPLOADER_WORKERS"),
		IngestTempDir:                         v.GetString("INGEST_TEMP_DIR"),
		IngestValidatorBufferSize:             v.GetInt("INGEST_VALIDATOR_BUFFER_SIZE"),
		IngestValidatorMaxAttempts:            v.GetInt("INGEST_VALIDATOR_MAX_ATTEMPTS"),
		IngestValidatorWorkers:                v.GetInt("INGEST_VALIDATOR_WORKERS"),
		LogDir:                                v.GetString("LOG_DIR"),
		LogLevel:                              getLogLevel(v.GetString("LOG_LEVEL")),
		MaxDaysSinceFixityCheck:               v.GetInt("MAX_DAYS_SINCE_LAST_FIXITY"),
		MaxFileSize:                           v.GetInt64("MAX_FILE_SIZE"),
		MaxFixityItemsPerRun:                  v.GetInt("MAX_FIXITY_ITEMS_PER_RUN"),
		MaxWorkerAttempts:                     v.GetInt("MAX_WORKER_ATTEMPTS"),
		NsqLookupd:                            v.GetString("NSQ_LOOKUPD"),
		NsqURL:                                v.GetString("NSQ_URL"),
		PharosAPIKey:                          v.GetString("PHAROS_API_KEY"),
		PharosAPIUser:                         v.GetString("PHAROS_API_USER"),
		PharosAPIVersion:                      v.GetString("PHAROS_API_VERSION"),
		PharosURL:                             v.GetString("PHAROS_URL"),
		ProfilesDir:                           v.GetString("PROFILES_DIR"),
		QueueFixityInterval:                   v.GetDuration("QUEUE_FIXITY_INTERVAL"),
		RedisDefaultDB:                        v.GetInt("REDIS_DEFAULT_DB"),
		RedisPassword:                         v.GetString("REDIS_PASSWORD"),
		RedisRetries:                          v.GetInt("REDIS_RETRIES"),
		RedisRetryMs:                          v.GetDuration("REDIS_RETRY_MS"),
		RedisURL:                              v.GetString("REDIS_URL"),
		RedisUser:                             v.GetString("REDIS_USER"),
		ReingestManagerBufferSize:             v.GetInt("REINGEST_MANAGER_BUFFER_SIZE"),
		ReingestManagerMaxAttempts:            v.GetInt("REINGEST_MANAGER_MAX_ATTEMPTS"),
		ReingestManagerWorkers:                v.GetInt("REINGEST_MANAGER_WORKERS"),
		RestoreDir:                            v.GetString("RESTORE_DIR"),
		S3AWSHost:                             v.GetString("S3_AWS_HOST"),
		S3Credentials: map[string]*S3Credentials{
			constants.StorageProviderAWS: &S3Credentials{
				Host:      v.GetString("S3_AWS_HOST"),
				KeyID:     v.GetString("S3_AWS_KEY"),
				SecretKey: v.GetString("S3_AWS_SECRET"),
			},
			constants.StorageProviderLocal: &S3Credentials{
				Host:      v.GetString("S3_LOCAL_HOST"),
				KeyID:     v.GetString("S3_LOCAL_KEY"),
				SecretKey: v.GetString("S3_LOCAL_SECRET"),
			},
			constants.StorageProviderWasabiOR: &S3Credentials{
				Host:      v.GetString("S3_WASABI_HOST_OR"),
				KeyID:     v.GetString("S3_WASABI_KEY"),
				SecretKey: v.GetString("S3_WASABI_SECRET"),
			},
			constants.StorageProviderWasabiVA: &S3Credentials{
				Host:      v.GetString("S3_WASABI_HOST_VA"),
				KeyID:     v.GetString("S3_WASABI_KEY"),
				SecretKey: v.GetString("S3_WASABI_SECRET"),
			},
		},
		S3LocalHost:          v.GetString("S3_LOCAL_HOST"),
		S3WasabiHostOR:       v.GetString("S3_WASABI_HOST_OR"),
		S3WasabiHostVA:       v.GetString("S3_WASABI_HOST_VA"),
		StagingBucket:        v.GetString("STAGING_BUCKET"),
		StagingUploadRetries: v.GetInt("STAGING_UPLOAD_RETRIES"),
		StagingUploadRetryMs: v.GetDuration("STAGING_UPLOAD_RETRY_MS"),
		VolumeServiceURL:     v.GetString("VOLUME_SERVICE_URL"),
	}
}

// CredentialsForS3Host returns the credentials for the specifed
// S3 host, or nil if no credentials exist for that host.
func (config *Config) CredentialsForS3Host(host string) (credentials *S3Credentials) {
	for _, c := range config.S3Credentials {
		if c.Host == host {
			credentials = c
		}
	}
	return credentials
}

// PreservationBucketFor returns the preservation buckets
// for the specified storage option.
// Storage options are enumerated in constants.StorageOptions. For most options,
// this will return a single item. For the Standard storage option, it returns
// two preservation buckets.
func (config *Config) PreservationBucketsFor(storageOption string) []*PreservationBucket {
	preservationBuckets := make([]*PreservationBucket, 0)
	for _, preservationBucket := range config.PreservationBuckets {
		if preservationBucket.OptionName == storageOption {
			preservationBuckets = append(preservationBuckets, preservationBucket)
		}
	}
	return preservationBuckets
}

// IsE2ETest returns true if the environment variable APT_E2E is set to "true".
// This is set only during end-to-end (E2E) tests so we can queue up some
// items for testing.
func (config *Config) IsE2ETest() bool {
	return os.Getenv("APT_E2E") == "true"
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
	if dir == dirName && strings.HasPrefix(dirName, ".") {
		// dirName didn't change
		absPath, err := filepath.Abs(path.Join(util.ProjectRoot(), dirName))
		if err == nil && absPath != "" {
			dir = absPath
		}
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
	if config.IngestBucketReaderInterval.Seconds() < float64(1) {
		util.PrintAndExit("Config is missing IngestBucketReaderInterval")
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

func (config *Config) checkPreservationBuckets() {
	for _, preservationBucket := range config.PreservationBuckets {
		if preservationBucket.Host == "" {
			util.PrintAndExit(fmt.Sprintf("S3 preservationBucket %s is missing Host", preservationBucket.OptionName))
		}
		if preservationBucket.Bucket == "" {
			util.PrintAndExit(fmt.Sprintf("S3 provider %s is missing Bucket", preservationBucket.OptionName))
		}
		if preservationBucket.Region == "" {
			util.PrintAndExit(fmt.Sprintf("S3 provider %s is missing Region", preservationBucket.OptionName))
		}
	}
}

func (config *Config) sanityCheck() {
	// If this is dev or test env, don't let config point
	// to any external services. This prevents a dev/test
	// installation from touching data in demo and prod systems.
	config.checkBasicSettings()
	config.checkS3Providers()
	config.checkPreservationBuckets()

	// This is turned off for now because of issues with docker,
	// where all services appear to our tests to run on external hosts.
	// config.checkHostSafety()
}

func (config *Config) makeDirs() error {
	dirs := []string{
		config.IngestTempDir,
		config.LogDir,
		config.RestoreDir,
	}
	for _, dir := range dirs {
		err := os.MkdirAll(dir, 0755)
		if err == nil || os.IsExist(err) {
			return nil
		} else {
			util.PrintAndExit(err.Error())
		}
	}
	return nil
}

func (config *Config) initPreservationBuckets() {
	config.PreservationBuckets = []*PreservationBucket{
		&PreservationBucket{
			Bucket:          config.BucketStandardVA,
			Description:     "AWS Virginia S3 bucket for Standard primary preservation",
			Host:            config.S3AWSHost,
			OptionName:      constants.StorageStandard,
			Provider:        constants.StorageProviderAWS,
			Region:          constants.RegionAWSUSEast1,
			RestorePriority: 1,
			StorageClass:    constants.StorageClassStandard,
		},
		&PreservationBucket{
			Bucket:          config.BucketStandardOR,
			Description:     "AWS Oregon Glacier bucket for Standard storage repilication",
			Host:            config.S3AWSHost,
			OptionName:      constants.StorageStandard,
			Provider:        constants.StorageProviderAWS,
			Region:          constants.RegionAWSUSWest2,
			RestorePriority: 4,
			StorageClass:    constants.StorageClassGlacier,
		},
		&PreservationBucket{
			Bucket:          config.BucketGlacierOH,
			Description:     "AWS Ohio Glacier storage",
			Host:            config.S3AWSHost,
			OptionName:      constants.StorageGlacierOH,
			Provider:        constants.StorageProviderAWS,
			Region:          constants.RegionAWSUSEast2,
			RestorePriority: 6,
			StorageClass:    constants.StorageClassGlacier,
		},
		&PreservationBucket{
			Bucket:          config.BucketGlacierOR,
			Description:     "AWS Oregon Glacier storage",
			Host:            config.S3AWSHost,
			OptionName:      constants.StorageGlacierOR,
			Provider:        constants.StorageProviderAWS,
			Region:          constants.RegionAWSUSWest2,
			RestorePriority: 7,
			StorageClass:    constants.StorageClassGlacier,
		},
		&PreservationBucket{
			Bucket:          config.BucketGlacierVA,
			Description:     "AWS Virginia Glacier storage",
			Host:            config.S3AWSHost,
			OptionName:      constants.StorageGlacierVA,
			Provider:        constants.StorageProviderAWS,
			Region:          constants.RegionAWSUSEast1,
			RestorePriority: 5,
			StorageClass:    constants.StorageClassGlacier,
		},
		&PreservationBucket{
			Bucket:          config.BucketGlacierDeepOH,
			Description:     "AWS Ohio Glacier deep storage",
			Host:            config.S3AWSHost,
			OptionName:      constants.StorageGlacierDeepOH,
			Provider:        constants.StorageProviderAWS,
			Region:          constants.RegionAWSUSEast2,
			RestorePriority: 9,
			StorageClass:    constants.StorageClassGlacierDeep,
		},
		&PreservationBucket{
			Bucket:          config.BucketGlacierDeepOR,
			Description:     "AWS Oregon Glacier deep storage",
			Host:            config.S3AWSHost,
			OptionName:      constants.StorageGlacierDeepOR,
			Provider:        constants.StorageProviderAWS,
			Region:          constants.RegionAWSUSWest2,
			RestorePriority: 10,
			StorageClass:    constants.StorageClassGlacierDeep,
		},
		&PreservationBucket{
			Bucket:          config.BucketGlacierDeepVA,
			Description:     "AWS Virginia Glacier deep storage",
			Host:            config.S3AWSHost,
			OptionName:      constants.StorageGlacierDeepVA,
			Provider:        constants.StorageProviderAWS,
			Region:          constants.RegionAWSUSEast1,
			RestorePriority: 8,
			StorageClass:    constants.StorageClassGlacierDeep,
		},
		&PreservationBucket{
			Bucket:          config.BucketWasabiOR,
			Description:     "Wasabi Oregon storage",
			Host:            config.S3WasabiHostOR,
			OptionName:      constants.StorageWasabiOR,
			Provider:        constants.StorageProviderWasabiOR,
			Region:          constants.RegionWasabiUSWest1,
			RestorePriority: 3,
			StorageClass:    constants.StorageClassWasabi,
		},
		&PreservationBucket{
			Bucket:          config.BucketWasabiVA,
			Description:     "Wasabi Virginia storage (us-east-1)",
			Host:            config.S3WasabiHostVA,
			OptionName:      constants.StorageWasabiVA,
			Provider:        constants.StorageProviderWasabiVA,
			Region:          constants.RegionWasabiUSEast1,
			RestorePriority: 2,
			StorageClass:    constants.StorageClassWasabi,
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

// BucketAndKeyFor returns the PreservationBucket object and S3 key
// for the specified URL.
func (config *Config) BucketAndKeyFor(urlStr string) (bucket *PreservationBucket, key string, err error) {
	_url, err := url.Parse(urlStr)
	if err != nil {
		return nil, "", err
	}
	parts := strings.SplitN(_url.Path, "/", 3) // parts[0] contains leading slash
	if len(parts) > 2 {
		key = parts[2]
	} else {
		return nil, "", fmt.Errorf("URL %s is missing key", urlStr)
	}
	for _, preservationBucket := range config.PreservationBuckets {
		if preservationBucket.HostsURL(urlStr) {
			bucket = preservationBucket
		}
	}
	if bucket == nil {
		return nil, "", fmt.Errorf("Cannot determine provider for URL %s", urlStr)
	}
	return bucket, key, nil
}
