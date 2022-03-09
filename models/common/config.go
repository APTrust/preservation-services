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
	APTQueueInterval           time.Duration
	BaseWorkingDir             string
	BucketGlacierDeepOH        string
	BucketGlacierDeepOR        string
	BucketGlacierDeepVA        string
	BucketGlacierOH            string
	BucketGlacierOR            string
	BucketGlacierVA            string
	BucketStandardOR           string
	BucketStandardVA           string
	BucketWasabiOR             string
	BucketWasabiVA             string
	ConfigFilePath             string
	ConfigName                 string
	IngestBucketReaderInterval time.Duration
	IngestTempDir              string
	LogDir                     string
	LogLevel                   logging.Level
	MaxDaysSinceFixityCheck    int
	MaxFileSize                int64
	MaxFixityItemsPerRun       int
	MaxWorkerAttempts          int
	NsqLookupd                 string
	NsqURL                     string
	PreservationBuckets        []*PreservationBucket
	ProfilesDir                string
	QueueFixityInterval        time.Duration
	RedisDefaultDB             int
	RedisPassword              string `json:"-"`
	RedisRetries               int
	RedisRetryMs               time.Duration
	RedisURL                   string
	RedisUser                  string `json:"-"`
	RegistryAPIKey             string `json:"-"`
	RegistryAPIUser            string `json:"-"`
	RegistryAPIVersion         string
	RegistryURL                string
	RestoreDir                 string
	S3AWSHost                  string
	S3Credentials              map[string]*S3Credentials `json:"-"`
	S3LocalHost                string
	S3WasabiHostOR             string
	S3WasabiHostVA             string
	StagingBucket              string
	StagingUploadRetryMs       time.Duration
	VolumeServiceURL           string
	WorkerSettings             map[string]int
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
	v.AutomaticEnv() // so env vars override file vars
	err := v.ReadInConfig()
	if err != nil {
		util.PrintAndExit(fmt.Sprintf("Fatal error config file: %v \n", err))
	}
	return &Config{
		APTQueueInterval:           v.GetDuration("APT_QUEUE_INTERVAL"),
		BaseWorkingDir:             v.GetString("BASE_WORKING_DIR"),
		BucketGlacierDeepOH:        v.GetString("BUCKET_GLACIER_DEEP_OH"),
		BucketGlacierDeepOR:        v.GetString("BUCKET_GLACIER_DEEP_OR"),
		BucketGlacierDeepVA:        v.GetString("BUCKET_GLACIER_DEEP_VA"),
		BucketGlacierOH:            v.GetString("BUCKET_GLACIER_OH"),
		BucketGlacierOR:            v.GetString("BUCKET_GLACIER_OR"),
		BucketGlacierVA:            v.GetString("BUCKET_GLACIER_VA"),
		BucketStandardOR:           v.GetString("BUCKET_STANDARD_OR"),
		BucketStandardVA:           v.GetString("BUCKET_STANDARD_VA"),
		BucketWasabiOR:             v.GetString("BUCKET_WASABI_OR"),
		BucketWasabiVA:             v.GetString("BUCKET_WASABI_VA"),
		ConfigFilePath:             path.Join(configDir, configFile),
		ConfigName:                 strings.Replace(configFile, ".env.", "", 1),
		IngestBucketReaderInterval: v.GetDuration("INGEST_BUCKET_READER_INTERVAL"),
		IngestTempDir:              v.GetString("INGEST_TEMP_DIR"),
		LogDir:                     v.GetString("LOG_DIR"),
		LogLevel:                   getLogLevel(v.GetString("LOG_LEVEL")),
		MaxDaysSinceFixityCheck:    v.GetInt("MAX_DAYS_SINCE_LAST_FIXITY"),
		MaxFileSize:                v.GetInt64("MAX_FILE_SIZE"),
		MaxFixityItemsPerRun:       v.GetInt("MAX_FIXITY_ITEMS_PER_RUN"),
		MaxWorkerAttempts:          v.GetInt("MAX_WORKER_ATTEMPTS"),
		NsqLookupd:                 v.GetString("NSQ_LOOKUPD"),
		NsqURL:                     v.GetString("NSQ_URL"),
		ProfilesDir:                v.GetString("PROFILES_DIR"),
		QueueFixityInterval:        v.GetDuration("QUEUE_FIXITY_INTERVAL"),
		RedisDefaultDB:             v.GetInt("REDIS_DEFAULT_DB"),
		RedisPassword:              v.GetString("REDIS_PASSWORD"),
		RedisRetries:               v.GetInt("REDIS_RETRIES"),
		RedisRetryMs:               v.GetDuration("REDIS_RETRY_MS"),
		RedisURL:                   v.GetString("REDIS_URL"),
		RedisUser:                  v.GetString("REDIS_USER"),
		RegistryAPIKey:             v.GetString("PRESERV_REGISTRY_API_KEY"),
		RegistryAPIUser:            v.GetString("PRESERV_REGISTRY_API_USER"),
		RegistryAPIVersion:         v.GetString("PRESERV_REGISTRY_API_VERSION"),
		RegistryURL:                v.GetString("PRESERV_REGISTRY_URL"),
		RestoreDir:                 v.GetString("RESTORE_DIR"),
		S3AWSHost:                  v.GetString("S3_AWS_HOST"),
		S3Credentials: map[string]*S3Credentials{
			constants.StorageProviderAWS: {
				Host:      v.GetString("S3_AWS_HOST"),
				KeyID:     v.GetString("S3_AWS_KEY"),
				SecretKey: v.GetString("S3_AWS_SECRET"),
			},
			constants.StorageProviderLocal: {
				Host:      v.GetString("S3_LOCAL_HOST"),
				KeyID:     v.GetString("S3_LOCAL_KEY"),
				SecretKey: v.GetString("S3_LOCAL_SECRET"),
			},
			constants.StorageProviderWasabiOR: {
				Host:      v.GetString("S3_WASABI_HOST_OR"),
				KeyID:     v.GetString("S3_WASABI_KEY"),
				SecretKey: v.GetString("S3_WASABI_SECRET"),
			},
			constants.StorageProviderWasabiVA: {
				Host:      v.GetString("S3_WASABI_HOST_VA"),
				KeyID:     v.GetString("S3_WASABI_KEY"),
				SecretKey: v.GetString("S3_WASABI_SECRET"),
			},
		},
		S3LocalHost:          v.GetString("S3_LOCAL_HOST"),
		S3WasabiHostOR:       v.GetString("S3_WASABI_HOST_OR"),
		S3WasabiHostVA:       v.GetString("S3_WASABI_HOST_VA"),
		StagingBucket:        v.GetString("STAGING_BUCKET"),
		StagingUploadRetryMs: v.GetDuration("STAGING_UPLOAD_RETRY_MS"),
		VolumeServiceURL:     v.GetString("VOLUME_SERVICE_URL"),
		WorkerSettings: map[string]int{
			constants.TopicDelete + "BufferSize":                 v.GetInt("APT_DELETE_BUFFER_SIZE"),
			constants.TopicDelete + "MaxAttempts":                v.GetInt("APT_DELETE_MAX_ATTEMPTS"),
			constants.TopicDelete + "Workers":                    v.GetInt("APT_DELETE_WORKERS"),
			constants.TopicFixity + "BufferSize":                 v.GetInt("APT_FIXITY_BUFFER_SIZE"),
			constants.TopicFixity + "MaxAttempts":                v.GetInt("APT_FIXITY_MAX_ATTEMPTS"),
			constants.TopicFixity + "Workers":                    v.GetInt("APT_FIXITY_WORKERS"),
			constants.TopicObjectRestore + "BufferSize":          v.GetInt("BAG_RESTORER_BUFFER_SIZE"),
			constants.TopicObjectRestore + "MaxAttempts":         v.GetInt("BAG_RESTORER_MAX_ATTEMPTS"),
			constants.TopicObjectRestore + "Workers":             v.GetInt("BAG_RESTORER_WORKERS"),
			constants.TopicFileRestore + "BufferSize":            v.GetInt("FILE_RESTORER_BUFFER_SIZE"),
			constants.TopicFileRestore + "MaxAttempts":           v.GetInt("FILE_RESTORER_MAX_ATTEMPTS"),
			constants.TopicFileRestore + "Workers":               v.GetInt("FILE_RESTORER_WORKERS"),
			constants.TopicGlacierRestore + "BufferSize":         v.GetInt("GLACIER_RESTORER_BUFFER_SIZE"),
			constants.TopicGlacierRestore + "MaxAttempts":        v.GetInt("GLACIER_RESTORER_MAX_ATTEMPTS"),
			constants.TopicGlacierRestore + "Workers":            v.GetInt("GLACIER_RESTORER_WORKERS"),
			constants.IngestCleanup + "BufferSize":               v.GetInt("INGEST_CLEANUP_BUFFER_SIZE"),
			constants.IngestCleanup + "MaxAttempts":              v.GetInt("INGEST_CLEANUP_MAX_ATTEMPTS"),
			constants.IngestCleanup + "Workers":                  v.GetInt("INGEST_CLEANUP_WORKERS"),
			constants.IngestFormatIdentification + "BufferSize":  v.GetInt("INGEST_FORMAT_IDENTIFIER_BUFFER_SIZE"),
			constants.IngestFormatIdentification + "MaxAttempts": v.GetInt("INGEST_FORMAT_IDENTIFIER_MAX_ATTEMPTS"),
			constants.IngestFormatIdentification + "Workers":     v.GetInt("INGEST_FORMAT_IDENTIFIER_WORKERS"),
			constants.IngestPreFetch + "BufferSize":              v.GetInt("INGEST_PRE_FETCH_BUFFER_SIZE"),
			constants.IngestPreFetch + "MaxAttempts":             v.GetInt("INGEST_PRE_FETCH_MAX_ATTEMPTS"),
			constants.IngestPreFetch + "Workers":                 v.GetInt("INGEST_PRE_FETCH_WORKERS"),
			constants.IngestStorage + "BufferSize":               v.GetInt("INGEST_PRESERVATION_UPLOADER_BUFFER_SIZE"),
			constants.IngestStorage + "MaxAttempts":              v.GetInt("INGEST_PRESERVATION_UPLOADER_MAX_ATTEMPTS"),
			constants.IngestStorage + "Workers":                  v.GetInt("INGEST_PRESERVATION_UPLOADER_WORKERS"),
			constants.IngestStorageValidation + "BufferSize":     v.GetInt("INGEST_PRESERVATION_VERIFIER_BUFFER_SIZE"),
			constants.IngestStorageValidation + "MaxAttempts":    v.GetInt("INGEST_PRESERVATION_VERIFIER_MAX_ATTEMPTS"),
			constants.IngestStorageValidation + "Workers":        v.GetInt("INGEST_PRESERVATION_VERIFIER_WORKERS"),
			constants.IngestRecord + "BufferSize":                v.GetInt("INGEST_RECORDER_BUFFER_SIZE"),
			constants.IngestRecord + "MaxAttempts":               v.GetInt("INGEST_RECORDER_MAX_ATTEMPTS"),
			constants.IngestRecord + "Workers":                   v.GetInt("INGEST_RECORDER_WORKERS"),
			constants.IngestStaging + "BufferSize":               v.GetInt("INGEST_STAGING_UPLOADER_BUFFER_SIZE"),
			constants.IngestStaging + "MaxAttempts":              v.GetInt("INGEST_STAGING_UPLOADER_MAX_ATTEMPTS"),
			constants.IngestStaging + "Workers":                  v.GetInt("INGEST_STAGING_UPLOADER_WORKERS"),
			constants.IngestValidation + "BufferSize":            v.GetInt("INGEST_VALIDATOR_BUFFER_SIZE"),
			constants.IngestValidation + "MaxAttempts":           v.GetInt("INGEST_VALIDATOR_MAX_ATTEMPTS"),
			constants.IngestValidation + "Workers":               v.GetInt("INGEST_VALIDATOR_WORKERS"),
			constants.IngestReingestCheck + "BufferSize":         v.GetInt("REINGEST_MANAGER_BUFFER_SIZE"),
			constants.IngestReingestCheck + "MaxAttempts":        v.GetInt("REINGEST_MANAGER_MAX_ATTEMPTS"),
			constants.IngestReingestCheck + "Workers":            v.GetInt("REINGEST_MANAGER_WORKERS"),
		},
	}
}

// GetWorkerSettings returns the buffer size, max attempts and number
// of workers (go routines) for the specified worker. The params
// bufSize, numWorkers, and maxAttempts CAN be passed in from the
// command line. When they have not been specified on the command line,
// they will have the value -1, and this method will return the value
// from the .env file instead. If they were specified on the command line
// (i.e. they're greater than zero), this will return the command line
// values instead. This allows the user to override the .env settings
// on the command line, if desired, or to just go with .env by not
// specifying overrides.
//
// Param workerName is actually a worker or queue topic name from the
// constants file. If you pass an invalid name, the system will panic
// at startup, which is preferable to running a service with undefined
// or invalid params.
func (config *Config) GetWorkerSettings(workerName string, bufSizeArg, numWorkersArg, maxAttemptsArg int) (bufSize, numWorkers, maxAttempts int) {
	if bufSizeArg > 0 {
		bufSize = bufSizeArg
	} else {
		bufSize = config.WorkerSettings[workerName+"BufferSize"]
	}
	if numWorkersArg > 0 {
		numWorkers = numWorkersArg
	} else {
		numWorkers = config.WorkerSettings[workerName+"Workers"]
	}
	if maxAttemptsArg > 0 {
		maxAttempts = maxAttemptsArg
	} else {
		maxAttempts = config.WorkerSettings[workerName+"MaxAttempts"]
	}
	return bufSize, numWorkers, maxAttempts
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
		if !isLocalHost(config.RegistryURL) {
			util.PrintAndExit(fmt.Sprintf("Dev/Test setup cannot point to external Registry instance %s", config.RegistryURL))
		}
		if !isLocalHost(config.RegistryURL) {
			util.PrintAndExit(fmt.Sprintf("Dev/Test setup cannot point to external Registry instance %s", config.RegistryURL))
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
	if config.RegistryAPIKey == "" {
		util.PrintAndExit("Config is missing RegistryAPIKey")
	}
	if config.RegistryAPIUser == "" {
		util.PrintAndExit("Config is missing RegistryAPIUser")
	}
	if config.RegistryAPIVersion == "" {
		util.PrintAndExit("Config is missing RegistryAPIVersion")
	}
	if config.RegistryURL == "" {
		util.PrintAndExit("Config is missing RegistryURL")
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
	if config.RegistryAPIKey == "" {
		util.PrintAndExit("Config is missing RegistryAPIKey")
	}
	if config.RegistryAPIUser == "" {
		util.PrintAndExit("Config is missing RegistryAPIUser")
	}
	if config.RegistryAPIVersion == "" {
		util.PrintAndExit("Config is missing RegistryAPIVersion")
	}
	if config.RegistryURL == "" {
		util.PrintAndExit("Config is missing RegistryURL")
	}
	if config.RestoreDir == "" {
		util.PrintAndExit("Config is missing RestoreDir")
	}
	if config.StagingBucket == "" {
		util.PrintAndExit("Config is missing StagingBucket")
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
		config.RestoreDir,
	}
	if config.LogDir != "STDOUT" {
		dirs = append(dirs, config.LogDir)
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
		{
			Bucket:          config.BucketStandardVA,
			Description:     "AWS Virginia S3 bucket for Standard primary preservation",
			Host:            config.S3AWSHost,
			OptionName:      constants.StorageStandard,
			Provider:        constants.StorageProviderAWS,
			Region:          constants.RegionAWSUSEast1,
			RestorePriority: 1,
			StorageClass:    constants.StorageClassStandard,
		},
		{
			Bucket:          config.BucketStandardOR,
			Description:     "AWS Oregon Glacier bucket for Standard storage repilication",
			Host:            config.S3AWSHost,
			OptionName:      constants.StorageStandard,
			Provider:        constants.StorageProviderAWS,
			Region:          constants.RegionAWSUSWest2,
			RestorePriority: 4,
			StorageClass:    constants.StorageClassGlacier,
		},
		{
			Bucket:          config.BucketGlacierOH,
			Description:     "AWS Ohio Glacier storage",
			Host:            config.S3AWSHost,
			OptionName:      constants.StorageGlacierOH,
			Provider:        constants.StorageProviderAWS,
			Region:          constants.RegionAWSUSEast2,
			RestorePriority: 6,
			StorageClass:    constants.StorageClassGlacier,
		},
		{
			Bucket:          config.BucketGlacierOR,
			Description:     "AWS Oregon Glacier storage",
			Host:            config.S3AWSHost,
			OptionName:      constants.StorageGlacierOR,
			Provider:        constants.StorageProviderAWS,
			Region:          constants.RegionAWSUSWest2,
			RestorePriority: 7,
			StorageClass:    constants.StorageClassGlacier,
		},
		{
			Bucket:          config.BucketGlacierVA,
			Description:     "AWS Virginia Glacier storage",
			Host:            config.S3AWSHost,
			OptionName:      constants.StorageGlacierVA,
			Provider:        constants.StorageProviderAWS,
			Region:          constants.RegionAWSUSEast1,
			RestorePriority: 5,
			StorageClass:    constants.StorageClassGlacier,
		},
		{
			Bucket:          config.BucketGlacierDeepOH,
			Description:     "AWS Ohio Glacier deep storage",
			Host:            config.S3AWSHost,
			OptionName:      constants.StorageGlacierDeepOH,
			Provider:        constants.StorageProviderAWS,
			Region:          constants.RegionAWSUSEast2,
			RestorePriority: 9,
			StorageClass:    constants.StorageClassGlacierDeep,
		},
		{
			Bucket:          config.BucketGlacierDeepOR,
			Description:     "AWS Oregon Glacier deep storage",
			Host:            config.S3AWSHost,
			OptionName:      constants.StorageGlacierDeepOR,
			Provider:        constants.StorageProviderAWS,
			Region:          constants.RegionAWSUSWest2,
			RestorePriority: 10,
			StorageClass:    constants.StorageClassGlacierDeep,
		},
		{
			Bucket:          config.BucketGlacierDeepVA,
			Description:     "AWS Virginia Glacier deep storage",
			Host:            config.S3AWSHost,
			OptionName:      constants.StorageGlacierDeepVA,
			Provider:        constants.StorageProviderAWS,
			Region:          constants.RegionAWSUSEast1,
			RestorePriority: 8,
			StorageClass:    constants.StorageClassGlacierDeep,
		},
		{
			Bucket:          config.BucketWasabiOR,
			Description:     "Wasabi Oregon storage",
			Host:            config.S3WasabiHostOR,
			OptionName:      constants.StorageWasabiOR,
			Provider:        constants.StorageProviderWasabiOR,
			Region:          constants.RegionWasabiUSWest1,
			RestorePriority: 3,
			StorageClass:    constants.StorageClassWasabi,
		},
		{
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
// It omits some sensitive data, such as the Registry API key and
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
