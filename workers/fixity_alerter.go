package workers

import (
	"os"

	"github.com/APTrust/preservation-services/models/common"
)

// FixityAlerter calls the admin API endpoint that generates alerts
// for failed fixity checks. Alerts go to institutional and APTrust
// admins. The alerter should run once a day.
type FixityAlerter struct {
	Context *common.Context
}

// NewFixityAlerter returns a new fixity alerter.
func NewFixityAlerter() *FixityAlerter {
	return &FixityAlerter{
		Context: common.NewContext(),
	}
}

// Run calls the Registry API endpoint to generate failed fixity
// alerts and logs the results. If Registry returns an error, this
// will exit with code 1; otherwise, it exits with code zero.
//
// Note that unlike other workers that run forever (or until killed),
// this one runs once and then exits. A typical run should take less
// than one second when Registry detects no failed fixity checks.
// If Registry does detect failed fixity checks, this may take 3-10
// seconds to run. In any case, this process is short-lived.
func (q *FixityAlerter) Run() {
	q.Context.Logger.Info("Starting with config settings:")
	q.Context.Logger.Info(q.Context.Config.ToJSON())

	failureCount := int64(0)

	q.Context.Logger.Info("Calling Registry endpoint to generate failed fixity alerts.")
	resp := q.Context.RegistryClient.GenerateFailedFixityAlerts()
	if resp.Error != nil {
		q.Context.Logger.Errorf("Registry returned error: %v", resp.Error)
		os.Exit(1)
	} else {
		q.Context.Logger.Info("Got valid response from Registry")
		for _, summary := range resp.FailedFixitySummaries() {
			if summary.Failures > 0 {
				q.Context.Logger.Warningf("Inst %d (%s): %d failed fixity checks",
					summary.InstitutionID, summary.InstitutionName, summary.Failures)
			} else {
				q.Context.Logger.Infof("Inst %d (%s): %d failed fixity checks",
					summary.InstitutionID, summary.InstitutionName, summary.Failures)
			}
			failureCount += summary.Failures
		}
	}

	if failureCount > 0 {
		q.Context.Logger.Errorf("Registry shows %d failed fixity checks.", failureCount)
	} else {
		q.Context.Logger.Info("No failed fixity checks.")
	}
}
