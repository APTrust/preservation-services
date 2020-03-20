package util_test

import (
	"fmt"
	"github.com/APTrust/preservation-services/util"
	"github.com/stretchr/testify/assert"
	//"github.com/stretchr/testify/require"
	//"os"
	"strings"
	"testing"
)

func TestSystemHasIdentifierPrograms(t *testing.T) {
	ok, missing := util.SystemHasIdentifierPrograms()
	assert.True(t, ok)
	assert.Equal(t, 0, len(missing),
		fmt.Sprintf("Missing: %s", strings.Join(missing, ", ")))
}
