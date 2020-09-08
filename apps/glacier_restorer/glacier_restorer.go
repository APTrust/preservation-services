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

	if opts.MaxAttempts < 4 {
		opts.MaxAttempts = 4
	}

	// If anything goes wrong, this panics.
	// Otherwise, it starts handling NSQ messages immediately.
	worker := workers.NewGlacierRestorer(
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
glacier_restorer initiates the Glacier restoration process, copying files
from Glacier to S3 so they can be restored. A typical restoration involves
at least one requeue. The restorer requests restoration from Glacier, requeues
the WorkItem, then checks again a few hours later to see if the restoration
is complete.

Once files have been restored from Glacier to S3, the worker creates a normal
restoration WorkItem, so the bag_restorer or file_restorer can restore the
file or object to the depositor's restoration bucket.

Note that MaxAttempts for this worker can be no less than 4, even if you try
to set it lower than that. Glacier restorations require periodic re-checks
every few hours, and if MaxAttempts is too low, the worker will say the item
was not restored simply because it didn't wait long enough.`
	fmt.Println(message)
	fmt.Println(cli.EnvMessage)
}
