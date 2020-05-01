package main

import (
	"fmt"
	"os"

	"github.com/APTrust/preservation-services/util/cli"
	//"github.com/APTrust/preservation-services/workers"
)

func main() {
	cli.Init()
	opts := cli.ParseOpts()
	cli.PrintDefaults()
	fmt.Println(opts)
	if opts.PrintHelp {
		printHelp()
		os.Exit(0)
	}
}

func printHelp() {
	fmt.Println(`Help message...`)
}
