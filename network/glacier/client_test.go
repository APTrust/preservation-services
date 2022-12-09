package glacier_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/network/glacier"
	"github.com/stretchr/testify/assert"
)

var expectedResponseCode = 202
var glacierURL = ""
var glacierPath = "/glacier-deep-oh/bucket/item-uuid"
var expectedBody = "<RestoreRequest><Days>10</Days><GlacierJobParameters><Tier>Standard</Tier></GlacierJobParameters></RestoreRequest>"

func TestGlacierRestore_200(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(getRestoreHandler(t)))
	defer testServer.Close()

	context := common.NewContext()

	glacierURL = fmt.Sprintf("%s%s", testServer.URL, glacierPath)

	statusCode, body, err := glacier.Restore(context, glacierURL)
	assert.Nil(t, err)
	assert.Equal(t, expectedResponseCode, statusCode)
	assert.Equal(t, "Hello Kitty", body)
}

func getRestoreHandler(t *testing.T) func(http.ResponseWriter, *http.Request) {
	keys := []string{
		"Content-Length",
		"X-Amz-Date",
		"Authorization",
		"X-Amz-Content-Sha256",
	}
	return func(w http.ResponseWriter, r *http.Request) {
		// Make sure URL is correct
		assert.Equal(t, glacierPath+"?restore=", r.URL.String())

		// Test the headers
		for _, key := range keys {
			assert.NotNil(t, r.Header[key], key)
			assert.Equal(t, 1, len(r.Header[key]), key)
		}

		// Test the request body
		body := new(strings.Builder)
		_, err := io.Copy(body, r.Body)
		defer r.Body.Close()
		assert.Nil(t, err)
		assert.Equal(t, expectedBody, body.String())

		// Return whatever is expected
		w.WriteHeader(expectedResponseCode)
		w.Write([]byte("Hello Kitty"))
	}
}
