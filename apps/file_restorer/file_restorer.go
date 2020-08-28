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
	worker := workers.NewFileRestorer(
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
bag_restorer restores individual files to a depositor's restoration bucket.`
	fmt.Println(message)
	fmt.Println(cli.EnvMessage)
}
