package cli

import (
	"flag"
	"os"
	"time"
)

type Options struct {
	ChannelBufferSize int
	ConfigDir         string
	ConfigName        string
	MaxAttempts       int
	NumWorkers        int
	PrintHelp         bool
	RequeueTimeout    time.Duration
}

var opts = Options{}
var defaultAttempts = 3
var defaultBufSize = 20
var defaultWorkers = 2
var defaultTimeout = 1 * time.Minute

func Init() {
	flag.IntVar(&opts.ChannelBufferSize, "bufsize", defaultBufSize, "Channel buffer size for go workers")
	flag.StringVar(&opts.ConfigDir, "config-dir", "", "The directory in which to find the APTrust services config file")
	flag.StringVar(&opts.ConfigName, "config-name", "", "The name of the configuration to load from config-dir. E.g. 'test' loads .env.test; 'demo' loads .env.demo")
	flag.IntVar(&opts.MaxAttempts, "max-attempts", defaultAttempts, "Maximum number of times a worker should attempt to process an item")
	flag.IntVar(&opts.NumWorkers, "workers", defaultWorkers, "Number of go routines to handle main processing work")
	flag.BoolVar(&opts.PrintHelp, "help", false, "Print help message")
	flag.DurationVar(&opts.RequeueTimeout, "requeue-timeout", defaultTimeout, "Requeue timeout for reprocessing items with non-fatal errors. Format examples: 500ms, 12s, 10m, 3m30s, 3h")
}

func ParseOpts() Options {
	flag.Parse()
	if os.Getenv("APT_CONFIG_DIR") == "" {
		os.Setenv("APT_CONFIG_DIR", opts.ConfigDir)
	}
	if os.Getenv("APT_SERVICES_CONFIG") == "" {
		os.Setenv("APT_SERVICES_CONFIG", opts.ConfigName)
	}
	return opts
}

func PrintDefaults() {
	flag.PrintDefaults()
}