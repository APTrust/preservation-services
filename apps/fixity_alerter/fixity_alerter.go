package main

import (
	"github.com/APTrust/preservation-services/workers"
)

func main() {
	alerter := workers.NewFixityAlerter()
	alerter.Run()
}
