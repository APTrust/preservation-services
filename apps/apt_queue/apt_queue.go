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

	reader := workers.NewAPTQueue()

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
apt_queue queues WorkItems for deletion and restoration in NSQ

When running as a service (i.e. withouth --run-once), this relies on the
config setting INGEST_APT_QUEUE_INTERVAL to determine how long to wait
after the end of one scan before beginning the next.

You can also run apt_queue as a one-off job with the --run-once
flag. It will perform one scan and then exit.
`
	fmt.Println(message)
	fmt.Println(cli.EnvMessage)
}
