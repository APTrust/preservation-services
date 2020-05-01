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
	worker := workers.NewIngestCleanup(
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
ingest_cleanup cleans up all processing data after completion of an ingest.
This includes files in the staging area, metadata collected in Redis, and
the original tar file in the depisitor's receiving bucket. In some cases,
ingest_cleanup may not delete the original bag from the receiving bucket.
This happens, for example, when the system thinks the tar file in receiving
is no longer the same as the one it ingested, or when errors other than
invalid bag errors occur.`
	fmt.Println(message)
	fmt.Println(cli.EnvMessage)
}
