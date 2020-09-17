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

	fileIdentifier := ""
	if len(os.Args) > 1 {
		fileIdentifier = os.Args[1]
		runOnce = true
	}

	if help {
		printHelp()
		os.Exit(0)
	}

	reader := workers.NewQueueFixity(fileIdentifier)

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
apt_queue_fixity queues GenericFiles for fixity checks.

When running as a service (i.e. withouth --run-once), this relies on the
config setting QUEUE_FIXITY_INTERVAL to determine how long to wait
after the end of one scan before beginning the next.

Config setting MAX_FIXITY_ITEMS_PER_RUN determines the maximum number
of items to queue in a single run.

You can also run this as a one-off job with the --run-once
flag. It will perform one scan and then exit.

You can also supply a command-line argment to queue only files whose
identifiers are like the given string. For example, this will queue
files whose identifiers begin with the specified string:

$ apt_queue_fixity 'test.edu/bag-of-photots/data/image01'

While this will queue the file matching the exact identifier:

$ apt_queue_fixity 'test.edu/bag-of-photots/data/image01.jpg'

If you do specify a file identifier, this app will run in --run-once
mode, since it doesn't make sense to queue the same file ever hour.
`
	fmt.Println(message)
	fmt.Println(cli.EnvMessage)
}
