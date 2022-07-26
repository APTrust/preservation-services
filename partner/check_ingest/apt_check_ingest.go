package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/network"
	"github.com/APTrust/preservation-services/partner/common"
	"github.com/APTrust/preservation-services/util"
	"github.com/op/go-logging"
)

var ValidEnvironments = []string{
	"demo",
	"local",
	"production",
	"staging",
}

type OutputObject struct {
	WorkItem           *registry.WorkItem
	IntellectualObject *registry.IntellectualObject
}

// apt_check_ingest returns information about whether an ingest
// has been completed.
func main() {
	fileToCheck := ""
	opts := getUserOptions()
	if opts.Debug {
		printOpts(opts)
	}
	if opts.HasErrors() {
		fmt.Fprintln(os.Stderr, opts.AllErrorsAsString())
		os.Exit(common.EXIT_USER_ERR)
	}
	args := flag.Args() // non-flag args
	if len(args) > 0 {
		fileToCheck = args[0]
	}
	if fileToCheck == "" {
		fmt.Fprintln(os.Stderr, "Missing required argument filename")
		fmt.Fprintln(os.Stderr, "Try: apt_check_ingest --help")
		os.Exit(common.EXIT_USER_ERR)
	}
	if opts.Debug {
		fmt.Printf("Filename: %s\n", fileToCheck)
		fmt.Println("----------------------------------------------")
	}
	logger := logging.MustGetLogger("apt_check_ingest")
	client, err := network.NewRegistryClient(
		opts.RegistryURL,
		common.RegistryAPIVersion,
		opts.APTrustAPIUser,
		opts.APTrustAPIKey,
		logger)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(common.EXIT_RUNTIME_ERR)
	}
	params := url.Values{}
	params.Set("name", fileToCheck)
	params.Set("action", constants.ActionIngest)
	params.Set("sort", "date_processed")
	if opts.ETag != "" {
		params.Set("etag", opts.ETag)
	}

	resp := client.WorkItemList(params)
	if opts.Debug {
		printResponse(resp, opts)
	}
	if resp.Error != nil {
		fmt.Fprintln(os.Stderr, resp.Error.Error())
		os.Exit(common.EXIT_RUNTIME_ERR)
	}
	items := resp.WorkItems()
	outputObjects := make([]OutputObject, len(items))
	for i, item := range items {
		outputObjects[i] = OutputObject{
			WorkItem: item,
		}
		if item.ObjectIdentifier != "" {
			resp = client.IntellectualObjectByIdentifier(item.ObjectIdentifier)
			if opts.Debug {
				printResponse(resp, opts)
			}
			if resp.Error != nil {
				fmt.Fprintln(os.Stderr, resp.Error.Error())
				os.Exit(common.EXIT_RUNTIME_ERR)
			}
			outputObjects[i].IntellectualObject = resp.IntellectualObject()
		}
	}
	if opts.OutputFormat == "text" {
		printText(outputObjects, fileToCheck, opts.ETag)
	} else {
		printJson(outputObjects)
	}
	os.Exit(exitCode(outputObjects))
}

func ingested(item *registry.WorkItem) bool {
	return (item.Stage == constants.StageCleanup &&
		item.Status == constants.StatusSuccess)
}

func exitCode(objects []OutputObject) int {
	if len(objects) == 0 {
		return common.EXIT_ITEM_NOT_FOUND
	}
	succeeded := false
	failed := false
	for _, obj := range objects {
		if ingested(obj.WorkItem) {
			succeeded = true
		} else {
			failed = true
		}
	}
	if succeeded && failed {
		return common.EXIT_SOME_INGESTED
	} else if succeeded {
		return common.EXIT_OK
	}
	return common.EXIT_NOT_INGESTED
}

func printText(objects []OutputObject, fileToCheck, etag string) {
	if len(objects) == 0 {
		if etag != "" {
			fmt.Println("No record for", fileToCheck, "with etag", etag)
		} else {
			fmt.Println("No record for", fileToCheck)
		}
	}
	for i, obj := range objects {
		objIdentifier := "<not ingested yet>"
		if obj.WorkItem.ObjectIdentifier != "" {
			objIdentifier = obj.WorkItem.ObjectIdentifier
		}
		fmt.Printf("%d) %s\n", i+1, obj.WorkItem.Name)
		fmt.Printf("    Etag:       %s\n", obj.WorkItem.ETag)
		fmt.Printf("    Size:       %d\n", obj.WorkItem.Size)
		fmt.Printf("    Updated:    %s\n", obj.WorkItem.UpdatedAt.Format(time.RFC3339))
		fmt.Printf("    Stage:      %s\n", obj.WorkItem.Stage)
		fmt.Printf("    Status:     %s\n", obj.WorkItem.Status)
		fmt.Printf("    Ingested:   %t\n", ingested(obj.WorkItem))
		fmt.Printf("    Identifier: %s\n", objIdentifier)
	}
}

func printJson(objects []OutputObject) {
	jsonBytes, err := json.Marshal(objects)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	fmt.Println(string(jsonBytes))
}

func printResponse(resp *network.RegistryResponse, opts *common.Options) {
	url := strings.Replace(resp.Request.URL.String(), "https:", opts.RegistryURL, 1)
	fmt.Println("----- HTTP Request -----")
	fmt.Println(resp.Request.Method, url)
	fmt.Println("Headers:")
	for k, v := range resp.Request.Header {
		fmt.Printf("  %s: %s\n", k, v[0])
	}
	fmt.Println("")
	fmt.Println("----- HTTP Response -----")
	fmt.Println("Headers:")
	for k, v := range resp.Response.Header {
		fmt.Printf("  %s: %s\n", k, v[0])
	}
	fmt.Println("\nBody:")
	respData, _ := resp.RawResponseData()
	fmt.Println(string(respData))
	fmt.Println("----------------------------------------------")
	fmt.Println("")
}

func printOpts(opts *common.Options) {
	configFile := opts.PathToConfigFile
	if configFile == "" {
		configFile = "<none>"
	}
	fmt.Println("Runtime options:")
	fmt.Println("  Config File:", configFile)
	fmt.Println("  APTrust API User:", opts.APTrustAPIUser)
	fmt.Println("  APTrust API Key:", opts.APTrustAPIKey)
	fmt.Println("  APTrust REST URL:", opts.RegistryURL)
	fmt.Println("  Output Format:", opts.OutputFormat)
	fmt.Println("  Debug:", opts.Debug)
	fmt.Println("----------------------------------------------")
	fmt.Println("")
}

// Get user-specified options from the command line,
// environment, and/or config file.
func getUserOptions() *common.Options {
	opts := parseCommandLine()
	opts.MergeConfigFileOptions()
	opts.VerifyOutputFormat()
	opts.VerifyRequiredAPICredentials()
	return opts
}

func parseCommandLine() *common.Options {
	var pathToConfigFile string
	var registryEnv string
	var outputFormat string
	var etag string
	var help bool
	var version bool
	var debug bool
	flag.StringVar(&pathToConfigFile, "config", "", "Path to partner config file")
	flag.StringVar(&registryEnv, "env", "production", "Which environment to query: production [default] or demo.")
	flag.StringVar(&outputFormat, "format", "text", "Output format ('text' or 'json')")
	flag.StringVar(&etag, "etag", "", "The etag of the bag you want to check on")
	flag.BoolVar(&help, "help", false, "Show help")
	flag.BoolVar(&version, "version", false, "Show version")
	flag.BoolVar(&debug, "debug", false, "Print debugging output to stdout")
	flag.Parse()

	if version {
		fmt.Println(common.GetVersion())
		os.Exit(common.EXIT_NO_OP)
	}
	if help {
		printUsage()
		os.Exit(common.EXIT_NO_OP)
	}

	if !util.StringListContains(ValidEnvironments, registryEnv) {
		fmt.Fprintln(os.Stderr, "Invalid value for -env:", registryEnv)
		printUsage()
		os.Exit(common.EXIT_USER_ERR)
	}

	registryUrl := "https://repo.aptrust.org"
	switch registryEnv {
	case "demo":
		registryUrl = "https://demo.aptrust.org"
	case "staging":
		// Note: The staging system is currently at registry.aptrust.org.
		// We should change this to registry.aptrust.org soon.
		registryUrl = "https://staging.aptrust.org"
	case "local":
		registryUrl = "http://localhost:8080"
	}
	return &common.Options{
		PathToConfigFile: pathToConfigFile,
		OutputFormat:     outputFormat,
		ETag:             etag,
		RegistryURL:      registryUrl,
		Debug:            debug,
	}
}

// Tell the user about the program.
func printUsage() {
	message := `
apt_check_ingest: Query APTrust REST API to discover whether a bag
has completed ingest. You'll need to set the variables
AptrustApiUser and AptrustApiKey in your APTrust config file.

APTrust issues API keys to users by request. The APTrust API user
is the email address of the user to whom the key was issued. If
you're using a config file, the required entries for user and API
key might look like this:

AptrustApiUser = "archivist@example.edu"
AptrustApiKey = "f887afc5e1624eda92ae1a5aecdf210c"

See https://wiki.aptrust.org/Partner_Tools for more info on the
APTrust config file.

Usage: apt_check_ingest [--config=<path to config file>] \
			[--env=<production|demo|staging>] \
			[--etag=<etag>] \
			[--format=<json|text>] \
			[--debug] <filename.tar>

       apt_check_ingest --version
       apt_check_ingest --help

Note that option flags may be preceded by either one or two dashes,
so -option is the same as --option.

Options

--config should point the APTrust partner config file that
  contains your user email and API key. If you don't want to specify the
  user and key in a config file, the program will try to read them from
  the environment keys APTRUST_API_USER and APTRUST_API_KEY.

--env specifies whether the tool should query the APTrust production
  system at https://repo.aptrust.org or the demo system at
  https://demo.aptrust.org. If unspecified, this defaults to production.

--etag is the AWS S3 etag assigned to a file upon upload to the
  receiving bucket. If you've uploaded multiple versions of a bag, each one
  will have a different etag. Specifying the etag here allows you to check
  on a single version of a bag that was uploaded multiple times.

--format specifies whether the result of the query should be printed
  to STDOUT in json or plain text format. Default is json.

--help prints this help message and exits.

--version prints version info and exits.

--debug prints information about the program's runtime options
  (including API user and API key) to STDOUT. It will also print the request
  sent to the APTrust REST server and the server's response.

Params

filename.tar is the name of the bag you uploaded for ingest. Use the filename
  only, not the full path to the bag. E.g., "virginia.edu.images.tar".

You will get multiple results for bags that have been ingested more than
once. For example, if you uploaded version 1 of a bag last year, and then
a newer version today, the output will include results for both bags,
with the most recent version listed first.

Exit codes:

0   - Bag or bags were successfully ingested
1   - Operation could not be completed due to runtime, network, or server error
4   - No record was found for the requested bag (or bag + etag)
3   - Operation could not be completed due to usage error (e.g. missing params)
5   - Bag or bags were not ingested
6   - Some bags have been ingested, some have not
100 - Printed help or version message. No other operations attempted.

Exit codes 0 and 5 indicate that ALL bags matching your query have (0)
or have not (5) been ingested. Exit code 6 indicates mixed results.
`
	fmt.Println(message)
}
