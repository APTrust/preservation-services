package cli

import (
	"flag"
	"time"
)

type Options struct {
	ChannelBufferSize int
	MaxAttempts       int
	PrintHelp         bool
	RequeueTimeout    time.Duration
}

var opts = Options{}
var defaultBufSize = 20
var defaultAttempts = 3
var defaultTimeout = 1 * time.Minute

func Init() {
	flag.IntVar(&opts.ChannelBufferSize, "bufsize", defaultBufSize, "Channel buffer size for go workers")
	flag.IntVar(&opts.MaxAttempts, "max-attempts", defaultAttempts, "Maximum number of times a worker should attempt to process an item")
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
