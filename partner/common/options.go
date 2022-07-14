package common

import (
	"fmt"
	//"github.com/APTrust/preservation-services/util"
	"os"
	"strings"
)

type Options struct {
	// PathToConfigFile is the path the APTrust partner config
	// file. If not specified, this defaults to ~/.aptrust_partner.conf.
	// This can be omitted entirely if you supply the -bucket and -key
	// options on the command line. Any required options not specified
	// on the command line will be pulled from this file.
	PathToConfigFile string
	// APTrustAPIKey is the key to connect to APTrust REST API.
	// The key must belong to APTrustAPIUser.
	APTrustAPIKey string
	// APTrustAPIKeyFrom tells whether the API key came from the config
	// file or the environment.
	APTrustAPIKeyFrom string
	// APTrustAPIKey is the user email address to connect to APTrust REST API.
	APTrustAPIUser string
	// APTrustAPIUserFrom tells whether the API user came from the config
	// file or the environment.
	APTrustAPIUserFrom string
	// ETag is the etag of an S3 upload. This is for tools that look up
	// bags by etag.
	ETag string
	// SecretAccessKey is the AWS Secret Access Key used to access your
	// S3 bucket.
	// RegistryURL is the URL of the Registry production or demo system.
	RegistryURL string
	// OutputFormat specifies how the program should print its results
	// to STDOUT. Options are "text" and "json".
	OutputFormat string
	// Debug indicates whether we should print debug output to Stdout.
	Debug bool
	// error contains a list of errors describing why these options are
	// not valid for an operation like upload or download.
	errors []string
}

// VerifyOutputFormat makes sure the user specified a valid output format.
func (opts *Options) VerifyOutputFormat() {
	if opts.OutputFormat != "text" && opts.OutputFormat != "json" {
		opts.addError("Param -format must be either 'text' or 'json'")
	}
}

func (opts *Options) VerifyRequiredAPICredentials() {
	if opts.APTrustAPIUser == "" {
		opts.addError("Cannot find APTrust API user in environment or config file")
	}
	if opts.APTrustAPIKey == "" {
		opts.addError("Cannot find APTrust API key in environment or config file")
	}
}

// MergeConfigFileOptions supplements command-line options with
// the default values the user specified in their APTrust
// parner config file.
//
// If the user left some options unspecified on the command line,
// load them from the config file, if we can. If the user specified
// a config file, use that. Otherwise, use the default config file
// in ~/.aptrust_partner.conf or %HOMEPATH%\.aptrust_partner.conf
func (opts *Options) MergeConfigFileOptions(action string) {
	partnerConfig := &PartnerConfig{}
	var err error
	partnerConfig, err = LoadPartnerConfig(opts.PathToConfigFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		return
	}
	if opts.APTrustAPIKey == "" && partnerConfig.APTrustAPIKey != "" {
		opts.APTrustAPIKey = partnerConfig.APTrustAPIKey
	}
	if opts.APTrustAPIUser == "" && partnerConfig.APTrustAPIUser != "" {
		opts.APTrustAPIUser = partnerConfig.APTrustAPIUser
	}
}

// addError adds an error to Options.Errors
func (opts *Options) addError(message string) {
	if opts.errors == nil {
		opts.errors = make([]string, 0)
	}
	opts.errors = append(opts.errors, message)
}

// Returns true of the options have any errors or missing
// required values.
func (opts *Options) HasErrors() bool {
	return opts.errors != nil && len(opts.errors) > 0
}

// AllErrorsAsString returns all errors as a single string,
// with each error ending in a newline. This is suitable
// for printing to STDOUT/STDERR.
func (opts *Options) AllErrorsAsString() string {
	errors := opts.Errors()
	if len(errors) > 0 {
		return strings.Join(errors, "\n")
	}
	return ""
}

// Errors returns a list of errors, such as invalid or
// missing params.
func (opts *Options) Errors() []string {
	if opts.errors == nil {
		opts.ClearErrors()
	}
	return opts.errors
}

// ClearErrors clears all errors. This is used in testing.
func (opts *Options) ClearErrors() {
	opts.errors = make([]string, 0)
}
