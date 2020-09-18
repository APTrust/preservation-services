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
	worker := workers.NewFixityChecker(
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
apt_fixity runs as a service to calculate fixity on items in preservation
storage. It reads generic file identifiers from the NSQ fixity queue,
calculates fixity on a single copy of a file in S3 (or non-Glacier) storage,
and records a PREMIS event with the result in Pharos.
`
	fmt.Println(message)
	fmt.Println(cli.EnvMessage)
}
