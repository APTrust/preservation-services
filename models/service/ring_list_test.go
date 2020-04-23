package service_test

import (
	"github.com/APTrust/preservation-services/models/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewRingList(t *testing.T) {
	ringList := service.NewRingList(10)
	assert.NotNil(t, ringList)
}

func TestAddAndContains(t *testing.T) {
	ringList := service.NewRingList(4)
	require.NotNil(t, ringList)

	ringList.Add("one")
	ringList.Add("two")
	ringList.Add("three")
	ringList.Add("four")
	assert.True(t, ringList.Contains("one"))
	assert.True(t, ringList.Contains("two"))
	assert.True(t, ringList.Contains("three"))
	assert.True(t, ringList.Contains("four"))

	ringList.Add("five")
	ringList.Add("six")

	// one and two should be pushed out by five and six
	assert.False(t, ringList.Contains("one"))
	assert.False(t, ringList.Contains("two"))

	assert.True(t, ringList.Contains("three"))
	assert.True(t, ringList.Contains("four"))
	assert.True(t, ringList.Contains("five"))
	assert.True(t, ringList.Contains("six"))
}

func TestDel(t *testing.T) {
	ringList := service.NewRingList(4)
	ringList.Add("one")
	ringList.Add("two")
	ringList.Add("three")
	ringList.Add("four")
	assert.True(t, ringList.Contains("one"))
	assert.True(t, ringList.Contains("two"))
	assert.True(t, ringList.Contains("three"))
	assert.True(t, ringList.Contains("four"))

	ringList.Del("one")
	ringList.Del("two")
	ringList.Del("three")
	ringList.Del("four")
	assert.False(t, ringList.Contains("one"))
	assert.False(t, ringList.Contains("two"))
	assert.False(t, ringList.Contains("three"))
	assert.False(t, ringList.Contains("four"))
}
