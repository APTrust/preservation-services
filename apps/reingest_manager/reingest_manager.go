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
	worker := workers.NewReingestManager(
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
reingest_manager checks to see if a bag has been previously ingested. If so,
it sets the preservation UUID on each file to match the already-preserved
file UUID, so we don't wind up with two copies. It also determines which files
need to be preserved. Those include new files and updated files. Files whose
checksums match the checksums of already preserved versions will not be copied
to preservation because they don't need to be updated.`
	fmt.Println(message)
	fmt.Println(cli.EnvMessage)
}
