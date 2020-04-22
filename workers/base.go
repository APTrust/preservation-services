package workers

import (
	"github.com/APTrust/preservation-services/models/common"
)

type Base struct {
	Context *common.Context

	// TODO: Channels for pre-process, process, post-process
	// Need to standardize Ingest Base interface first.
}
