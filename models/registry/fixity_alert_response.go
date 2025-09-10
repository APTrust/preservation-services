package registry

// FailedFixitySummary contains information about the number of
// failed fixity checks detected at a specific institution.
type FailedFixitySummary struct {
	Failures        int64  `json:"failures"`
	InstitutionID   int64  `json:"institution_id"`
	InstitutionName string `json:"institution_name"`
	Error           string `json:"error"`
}
