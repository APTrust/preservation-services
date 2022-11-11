package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/APTrust/preservation-services/audit/audit_core"
	"github.com/APTrust/preservation-services/models/common"
)

func main() {
	inputFile, doFullFixityCheck := parseFlags()
	ids := getFileIds(inputFile)
	aptCtx := common.NewContext()
	w := csv.NewWriter(os.Stdout)
	if err := w.Write(audit_core.CsvHeaders); err != nil {
		fmt.Fprintln(os.Stderr, "Error writing CSV headers:", err)
		os.Exit(2)
	}
	defer w.Flush()
	for _, id := range ids {
		auditor := audit_core.NewAuditor(aptCtx, id, doFullFixityCheck)
		record := auditor.Run()
		values := record.CsvValues()
		if err := w.Write(values); err != nil {
			fmt.Fprintln(os.Stderr, "Error writing CSV values:", err)
			fmt.Fprintln(os.Stderr, "Values were:", values)
		}
		if record.CheckPassed {
			fmt.Fprintln(os.Stderr, record.GenericFileID, "passed")
		} else {
			fmt.Fprintln(os.Stderr, record.GenericFileID, "failed")
		}
	}
}

func parseFlags() (string, bool) {
	var inputFile string
	var doFullFixityCheck bool
	flag.StringVar(&inputFile, "i", "", "Name of input file, with one generic file id per line.")
	flag.BoolVar(&doFullFixityCheck, "f", false, "Run full fixity check if necessary")
	flag.Parse()
	if inputFile == "" {
		fmt.Fprintln(os.Stderr, "Param -i (input file) is required")
		os.Exit(1)
	}
	return inputFile, doFullFixityCheck
}

func getFileIds(inputFile string) []int64 {
	data, err := os.ReadFile(inputFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Cannot read input file", inputFile, ":", err)
		os.Exit(1)
	}
	lines := strings.Split(string(data), "\n")
	ids := make([]int64, 0) // Because some numbers may not parse
	for i, line := range lines {
		numStr := strings.TrimSpace(line)
		if numStr == "" {
			continue
		}
		id, err := strconv.ParseInt(numStr, 10, 64)
		if err == nil {
			ids = append(ids, id)
		} else {
			fmt.Fprintln(os.Stderr, "Ignoring input file line", i+1, ":", numStr, "is not a number")
		}
	}
	return ids
}
