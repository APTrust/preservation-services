package bagit_test

import (
	"testing"

	"github.com/APTrust/preservation-services/bagit"
	"github.com/stretchr/testify/assert"
)

func TestTagDefinitionIsLegalValue(t *testing.T) {
	tagDef := &bagit.TagDefinition{
		Values: []string{"one", "two", "three"},
	}
	assert.True(t, tagDef.IsLegalValue("one"))
	assert.True(t, tagDef.IsLegalValue("two"))
	assert.False(t, tagDef.IsLegalValue("six"))

	// values are case-insensitive
	assert.True(t, tagDef.IsLegalValue("ONE"))
	assert.True(t, tagDef.IsLegalValue("Two"))

	// If Values is nil or empty, any value is legal
	tagDef.Values = make([]string, 0)
	assert.True(t, tagDef.IsLegalValue("homer"))
	assert.True(t, tagDef.IsLegalValue("marge"))

	tagDef.Values = nil
	assert.True(t, tagDef.IsLegalValue("homer"))
	assert.True(t, tagDef.IsLegalValue("marge"))
}
