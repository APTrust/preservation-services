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

// Note that if items whose defaults are set to -1 below are
// not specified on the command line, they will be pulled from
// the .env file. See util.GetWorkerSettings, which decides
// whether to use command-line or .env settings for attempts,
// bufSize and workers.
var opts = Options{}
var defaultAttempts = -1
var defaultBufSize = -1
var defaultWorkers = -1
var defaultTimeout = 1 * time.Minute

var EnvMessage = `
Before running this, set env variable APT_ENV
to the name of the configuration to load. For example:
    test - Loads .env.test
    demo - Loads .env.demo

The .env file should be in the current working directory
from which you launch the process.

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
