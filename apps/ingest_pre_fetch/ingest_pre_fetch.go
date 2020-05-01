package main

import (
	"fmt"
	"os"

	"github.com/APTrust/preservation-services/util/cli"
	"github.com/APTrust/preservation-services/workers"
)

func main() {
	cli.Init()
	opts := cli.ParseOpts()
	if opts.PrintHelp {
		printHelp()
		cli.PrintDefaults()
		os.Exit(0)
	}

	// If anything goes wrong, this panics.
	// Otherwise, it starts handling NSQ messages immediately.
	worker := workers.NewIngestPreFetch(
		opts.ChannelBufferSize,
		opts.NumWorkers,
		opts.MaxAttempts,
	)

	// This channel blocks until we get an interrupt,
	// so our program does not exit without Control-C
	// or other kill signal.
	<-worker.NSQConsumer.StopChan
}

func printHelp() {
	message := `
ingest_pre_fetch handles the first step of the ingest process, gathering
metadata from a tarred bag in a depositor's receiving bucket and saving
the metadata to Redis for subsequent workers.

If you don't set -config-dir and -config-name on the command line,
this requires the following environtment vars:

APT_CONFIG_DIR - Path to the directory containing the .env settings file.

APT_SERVICES_CONFIG - Name of the configuration to load. For example:
    test - Loads .env.test from APT_CONFIG_DIR
    demo - Loads .env.demo from APT_CONFIG_DIR
`
	fmt.Println(message)
}
