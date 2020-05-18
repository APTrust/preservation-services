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

	reader := workers.NewIngestBucketReader()
	reader.Run()
}

func printHelp() {
	message := `
ingest_bucket_reader scans depositors' receiving buckets for new bags to
ingest. It creates a new WorkItem for each new bag and queues the WorkItem
ID in the NSQ ingest pre-fetch topic.

Though this accepts the common ingest worker params bufsize, max-attempts,
and workers, it ignores them. Unlike other workers, ingest_bucket_reader
does not run as a service. It's meant to be run as a cron job, doing its
work and then exiting on completion. A single run can take a few seconds
for a handful of bags, or over an hour for thousands of bags.

`
	fmt.Println(message)
	fmt.Println(cli.EnvMessage)
}
