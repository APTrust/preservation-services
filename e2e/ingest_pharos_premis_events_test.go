// +build e2e

package e2e_test

import (
	"fmt"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/stretchr/testify/assert"
)

func testPremisEvents(pharosFile, expectedFile *registry.GenericFile) {
	assert.Equal(ctx.T, len(expectedFile.PremisEvents), len(pharosFile.PremisEvents))
	pharosEvents := hashEvents(pharosFile.PremisEvents)
	for _, event := range expectedFile.PremisEvents {
		key := eventKey(event)
		pharosEvent := pharosEvents[key]
		assert.NotNil(ctx.T, pharosEvent, "Pharos file %s is missing event %s", pharosFile.Identifier, key)
	}
}

// Use hash/map instead of repeated nested loop lookups
func hashEvents(events []*registry.PremisEvent) map[string]*registry.PremisEvent {
	eventMap := make(map[string]*registry.PremisEvent)
	for _, e := range events {
		eventMap[eventKey(e)] = e
	}
	return eventMap
}

// Unique key to match expected and actual events.
// Key must include type and outcome info, and must not include
// UUIDs that change on every ingest.
func eventKey(event *registry.PremisEvent) string {
	suffix := event.OutcomeDetail
	if event.EventType == constants.EventIdentifierAssignment || event.EventType == constants.EventReplication {
		suffix = event.OutcomeInformation
	}
	return fmt.Sprintf("%s / %s", event.EventType, suffix)
}