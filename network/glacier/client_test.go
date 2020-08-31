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
	//"github.com/stretchr/testify/require"
)

var expectedResponseCode = 0
var glacierURL = ""
var glacierPath = "/bucket/item-uuid"
var expectedBody = "<RestoreRequest><Days>10</Days><GlacierJobParameters><Tier>Standard</Tier></GlacierJobParameters></RestoreRequest>"

func TestGlacierRestore_200(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(getRestoreHandler(t)))
	defer testServer.Close()

	glacierURL = fmt.Sprintf("%s%s", testServer.URL, glacierPath)

	context := common.NewContext()
	//setTestServerPorts(context, testServer.URL)

	codes := []int{
		200,
		202,
		409,
		503,
	}

	for _, code := range codes {
		expectedResponseCode = code
		statusCode, err := glacier.Restore(context, glacierURL)
		assert.Nil(t, err)
		assert.Equal(t, code, statusCode)
	}
}

// func getGlacierURL(testServerURL string) string {
// 	port := strings.Split(testServerURL, ":")[2]
// 	return fmt.Sprintf("http://s3.us-west-2.localhost:%s/preservation-or/246c54b3-6476-494c-8fd0-bdd260b712c6", port)
// }

// func setTestServerPorts(context *common.Context, testServerURL string) {
// 	port := strings.Split(testServerURL, ":")[2]
// 	for _, bucket := range context.Config.PerservationBuckets {
// 		bucket.Host = strings.Replace(bucket.Host, "9899", port, 1)
// 	}
// }

func getRestoreHandler(t *testing.T) func(http.ResponseWriter, *http.Request) {
	keys := []string{
		"Content-Length",
		"X-Amz-Date",
		"Authorization",
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
	}
}
