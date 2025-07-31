package registry

// FixityAlertResponse is the response we receive from Registry
// after calling the admin API endpoint to generate failed
// fixity alerts. The Summaries property contains a summary report
// for each institution that had a failure.
type FixityAlertResponse struct {
	Error     string                 `json:"error"`
	Summaries []*FailedFixitySummary `json:"summaries"`
}

// FailedFixitySummary contains information about the number of
// failed fixity checks detected at a specific institution.
type FailedFixitySummary struct {
	Failures        int64  `json:"failures"`
	InstitutionID   int64  `json:"institution_id"`
	InstitutionName string `json:"institution_name"`
}
