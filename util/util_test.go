package util_test

import (
	"github.com/APTrust/preservation-services/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStringListContains(t *testing.T) {
	list := []string{"apple", "orange", "banana"}
	assert.True(t, util.StringListContains(list, "orange"))
	assert.False(t, util.StringListContains(list, "wedgie"))
	// Don't crash on nil list
	assert.False(t, util.StringListContains(nil, "mars"))
}
