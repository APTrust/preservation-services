package bagit_test

import (
	"github.com/APTrust/preservation-services/bagit"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCleanBagName(t *testing.T) {
	expected := "some.file"
	assert.Equal(t, expected, bagit.CleanBagName("some.file.b001.of200.tar"))
	assert.Equal(t, expected, bagit.CleanBagName("some.file.b1.of2.tar"))
	assert.Equal(t, expected, bagit.CleanBagName("some.file.tar"))
	assert.Equal(t, expected, bagit.CleanBagName("some.file"))
}
