package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/APTrust/preservation-services/util/cli"
	"github.com/APTrust/preservation-services/workers"
)

func main() {
	help := false
	runOnce := false
	flag.BoolVar(&help, "help", false, "Print help message")
	flag.BoolVar(&runOnce, "run-once", false, "Run once and exit (cron mode instead of server mode)")
	flag.Parse()

	if help {
		printHelp()
		os.Exit(0)
	}

	reader := workers.NewIngestBucketReader()

	if runOnce {
		reader.RunOnce()
	} else {
		stopChan := make(chan struct{})
		reader.RunAsService()
		<-stopChan
	}
}

func printHelp() {
	message := `
ingest_bucket_reader scans depositors' receiving buckets for new bags to
ingest. It creates a new WorkItem for each new bag and queues the WorkItem
ID in the NSQ ingest pre-fetch topic.

When running as a service (i.e. withouth --run-once), this relies on the
config setting INGEST_BUCKET_READER_INTERVAL to determine how long to wait
after the end of one scan before beginning the next.

You can also run the bucket reader as a one-off job with the --run-once
flag. It will perform one scan and then exit.
`
	fmt.Println(message)
	fmt.Println(cli.EnvMessage)
}
