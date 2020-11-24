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
	worker := workers.NewIngestRecorder(
		opts.ChannelBufferSize,
		// TODO: Remove hard-coded values once we can
		// pass values in on docker command line or
		// through .env file. See https://trello.com/c/SwLGgehH
		// For now, stick with a single worker, so we don't
		// overwhelm the staging server.
		1, //opts.NumWorkers,
		5, //opts.MaxAttempts,
	)

	// This channel blocks until we get an interrupt,
	// so our program does not exit without Control-C
	// or other kill signal.
	<-worker.NSQConsumer.StopChan
}

func printHelp() {
	message := `
ingest_recorder records all ingest metadata in Pharos. This includes
intellectual objects, generic files, premis events, and checksums.`
	fmt.Println(message)
	fmt.Println(cli.EnvMessage)
}
