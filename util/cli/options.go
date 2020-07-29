package cli

import (
	"flag"
	"time"
)

type Options struct {
	ChannelBufferSize int
	MaxAttempts       int
	NumWorkers        int
	PrintHelp         bool
	RequeueTimeout    time.Duration
}

var opts = Options{}
var defaultAttempts = 3
var defaultBufSize = 20
var defaultWorkers = 3
var defaultTimeout = 1 * time.Minute

var EnvMessage = `If you don't set -config-dir and -config-name on the command line,
this requires the following environtment vars:

APT_CONFIG_DIR - Path to the directory containing the .env settings file.

APT_ENV - Name of the configuration to load. For example:
    test - Loads .env.test from APT_CONFIG_DIR
    demo - Loads .env.demo from APT_CONFIG_DIR
`

func Init() {
	flag.IntVar(&opts.ChannelBufferSize, "bufsize", defaultBufSize, "Channel buffer size for go workers")
	flag.IntVar(&opts.MaxAttempts, "max-attempts", defaultAttempts, "Maximum number of times a worker should attempt to process an item")
	flag.IntVar(&opts.NumWorkers, "workers", defaultWorkers, "Number of go routines to handle main processing work")
	flag.BoolVar(&opts.PrintHelp, "help", false, "Print help message")
	flag.DurationVar(&opts.RequeueTimeout, "requeue-timeout", defaultTimeout, "Requeue timeout for reprocessing items with non-fatal errors. Format examples: 500ms, 12s, 10m, 3m30s, 3h")
}

func ParseOpts() Options {
	flag.Parse()
	return opts
}

func PrintDefaults() {
	flag.PrintDefaults()
}
