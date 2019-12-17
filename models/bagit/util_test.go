package bagit_test

import (
	"github.com/APTrust/perservation-services/models/bagit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCleanBagName(t *testing.T) {
	expected := "some.file"
	assert.Equal(t, expected, bagit.CleanBagName("some.file.b001.of200.tar"))
	assert.Equal(t, expected, bagit.CleanBagName("some.file.b1.of2.tar"))
	assert.Equal(t, expected, bagit.CleanBagName("some.file.tar"))
	assert.Equal(t, expected, bagit.CleanBagName("some.file"))
}
