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
		os.Exit(0)
	}

	// If anything goes wrong, this panics.
	// Otherwise, it starts handling NSQ messages immediately.
	workers.NewIngestPreFetch(
		opts.ChannelBufferSize,
		opts.NumWorkers,
		opts.MaxAttempts,
	)

}

func printHelp() {
	message := `
ingest_pre_fetch handles the first step of the ingest process, gathering
metadata from a tarred bag in a depositor's receiving bucket and saving
the metadata to Redis for subsequent workers.
`
	fmt.Println(message)
}
