package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/APTrust/preservation-services/util"
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

	err := checkPidFile()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	reader := workers.NewIngestBucketReader()
	reader.Run()
}

func checkPidFile(pidFile string) error {
	pidFile := "/var/tmp/ingest_bucket_reader.pid"
	otherPid := util.ReadPidFile(pidFile)
	if otherPid != 0 {
		if util.ProcessIsRunning(otherPid) {
			return fmt.Errorf("Bucket reader is already running with pid %d", otherPid)
		} else {
			util.DeletePidFile(pidFile)
		}
	}
	return util.WritePidFile(pidFile)
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

ingest_bucket_reader will not run if the pidfile at
/var/tmp/ingest_bucket_reader.pid and will not start if
`
	fmt.Println(message)
	fmt.Println(cli.EnvMessage)
}
