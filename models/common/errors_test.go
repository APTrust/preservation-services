package common_test

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/APTrust/preservation-services/models/common"
	"github.com/stretchr/testify/assert"
)

var msg = "Something went wrong"
var innerError = fmt.Errorf("This is the inner error")

func TestNewError(t *testing.T) {
	err := common.NewError(
		msg,
		nil,
		false,
	)
	assert.Nil(t, err.Err)
	assert.Equal(t, msg, err.Message)
	assert.Equal(t, msg, err.Error())
	assert.False(t, err.IsFatal)
	assert.NotEqual(t, 0, err.Line)
	assert.NotEqual(t, "", err.File)
}

func TestErrorUnwrap(t *testing.T) {
	err := common.NewError(
		msg,
		innerError,
		false,
	)
	assert.Equal(t, innerError, err.Unwrap())
}

func TestErrorFatal(t *testing.T) {
	err := common.NewError(
		msg,
		nil,
		true,
	)
	assert.True(t, err.IsFatal)
}

func TestErrorDetail(t *testing.T) {
	err := common.NewError(
		msg,
		innerError,
		true,
	)
	detail := err.Detail()
	assert.True(t, strings.HasPrefix(detail, "FATAL"))
	assert.True(t, strings.Contains(detail, err.Message))
	assert.True(t, strings.Contains(detail, err.File))
	assert.True(t, strings.Contains(detail, strconv.Itoa(err.Line)))
	assert.True(t, strings.Contains(detail, "Underlying error"))
	assert.True(t, strings.Contains(detail, innerError.Error()))
}

func TestNewHttpError(t *testing.T) {
	testURL := "https://example.com"
	err := common.NewHttpError(
		msg,
		innerError,
		http.MethodGet,
		testURL,
		http.StatusTeapot,
	)
	assert.Equal(t, msg, err.Message)
	assert.Equal(t, innerError, err.Err)
	assert.Equal(t, http.MethodGet, err.Method)
	assert.Equal(t, testURL, err.URL)
	assert.Equal(t, http.StatusTeapot, err.StatusCode)

	assert.Equal(t, msg, err.Error())
	assert.Equal(t, innerError, err.Unwrap())

	detail := err.Detail()
	assert.True(t, strings.Contains(detail, "returned status"))
	assert.True(t, strings.Contains(detail, err.Message))
	assert.True(t, strings.Contains(detail, strconv.Itoa(err.StatusCode)))
	assert.True(t, strings.Contains(detail, "Underlying error"))
	assert.True(t, strings.Contains(detail, innerError.Error()))
}
