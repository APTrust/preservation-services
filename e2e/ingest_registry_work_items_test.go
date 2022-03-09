//go:build e2e
// +build e2e

package e2e_test

import (
	"net/url"
	"strconv"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/e2e"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testWorkItemsAfterIngest() {
	t := ctx.T
	params := url.Values{}
	params.Set("action", constants.ActionIngest)
	params.Set("institution_id", strconv.FormatInt(ctx.TestInstitution.ID, 10))
	resp := ctx.Context.RegistryClient.WorkItemList(params)
	require.Nil(t, resp.Error)
	registryItems := resp.WorkItems()
	require.NotEmpty(t, registryItems)

	itemCounts := make(map[string]int)

	// 17 ingests plus 4 reingests
	assert.Equal(t, 21, len(registryItems))
	for _, item := range registryItems {
		assert.Equal(t, "Finished cleanup. Ingest complete.", item.Note)
		assert.Equal(t, constants.StageCleanup, item.Stage)
		assert.Equal(t, constants.StatusSuccess, item.Status)
		assert.Equal(t, "Ingest complete", item.Outcome)
		assert.False(t, item.BagDate.IsZero())
		assert.False(t, item.DateProcessed.IsZero())
		assert.False(t, item.QueuedAt.IsZero())
		assert.NotEmpty(t, item.ObjectIdentifier)
		assert.Empty(t, item.GenericFileIdentifier)
		assert.Empty(t, item.Node)
		assert.Equal(t, 0, item.Pid)
		assert.NotEmpty(t, item.InstitutionID)
		assert.NotEmpty(t, item.Size)
		assert.False(t, item.NeedsAdminReview)

		if _, ok := itemCounts[item.Name]; !ok {
			itemCounts[item.Name] = 0
		}
		itemCounts[item.Name]++
	}

	for _, bag := range e2e.ReingestBags() {
		count := itemCounts[bag.TarFileName()]
		assert.NotNil(t, count)
		assert.Equal(t, 2, count)
	}
}
