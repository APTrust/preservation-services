// +build e2e

package e2e_test

import (
	"net/url"
	"strings"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/e2e"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testFixityResults() {
	for _, testFile := range e2e.FilesForFixityCheck {
		event := getLatestFixityCheckEvent(testFile.Identifier)
		// This OutcomeInformation message is unique to successful
		// scheduled fixity check events.
		require.True(ctx.T, strings.HasPrefix(event.OutcomeInformation, "Fixity matches at"), testFile.Identifier)
		assert.Equal(ctx.T, constants.OutcomeSuccess, event.Outcome)
	}
}

func getLatestFixityCheckEvent(gfIdentifier string) *registry.PremisEvent {
	params := url.Values{}
	params.Set("file_identifier", gfIdentifier)
	params.Set("event_type", constants.EventFixityCheck)
	params.Set("page", "1")
	params.Set("per_page", "1")

	// When sort = "date", the PremisEventsController in Pharos
	// changes it to "date_time desc" which is bad design.
	// The sort param should be explicit, but if you specify
	// "date_time desc", the controller discards the param altogether.
	params.Set("sort", "date")

	resp := ctx.Context.PharosClient.PremisEventList(params)
	require.Nil(ctx.T, resp.Error, gfIdentifier)
	return resp.PremisEvent()
}