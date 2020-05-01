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
	worker := workers.NewStagingUploader(
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
staging_uploader unpacks files from a tarred bag in a depositor's receiving
bucket and copies the files to an S3 staging bucket. The files are copied to
staging using UUIDs instead of file names. They will have the same UUIDs when
they're later copied into preservation. In the staging bucket, the files are
stored with key <WorkItemID>/<UUID>.`
	fmt.Println(message)
	fmt.Println(cli.EnvMessage)
}
