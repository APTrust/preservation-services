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

	stopChan := make(chan struct{})

	reader := workers.NewIngestBucketReader()
	reader.Run()

	<-stopChan
}

func printHelp() {
	message := `
ingest_bucket_reader scans depositors' receiving buckets for new bags to
ingest. It creates a new WorkItem for each new bag and queues the WorkItem
ID in the NSQ ingest pre-fetch topic.

Though this accepts the common ingest worker params bufsize, max-attempts,
and workers, it ignores them. It relies on the config setting
INGEST_BUCKET_READER_INTERVAL to determine how long to wait after the end
of one scan before beginning the next.

`
	fmt.Println(message)
	fmt.Println(cli.EnvMessage)
}
