package testutil_test

import (
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"
)

const expectedText = "Test http file\n"

var headers = map[string]string{
	"Header1": "Value1",
	"Header2": "Value2",
}

// Should return the contents of the file at testdata/test_http_file.txt,
// along with the specified headers.
func TestHttpFileResponder(t *testing.T) {
	filePath := path.Join(testutil.PathToTestData(), "test_http_file.txt")
	handler := testutil.HttpFileResponder(headers, filePath)
	testServer := httptest.NewServer(handler)
	defer testServer.Close()

	resp, err := http.Get(testServer.URL)
	require.Nil(t, err)

	assertEqualHeaders(t, resp)

	data := getResponseBody(t, resp)
	assert.Equal(t, expectedText, string(data))
}

// Should return the string expectedText.
func TestHttpStringResponder(t *testing.T) {
	handler := testutil.HttpStringResponder(headers, expectedText)
	testServer := httptest.NewServer(handler)
	defer testServer.Close()

	resp, err := http.Get(testServer.URL)
	require.Nil(t, err)

	assertEqualHeaders(t, resp)

	data := getResponseBody(t, resp)
	assert.Equal(t, expectedText, string(data))
}

func assertEqualHeaders(t *testing.T, resp *http.Response) {
	assert.Equal(t, "Value1", resp.Header.Get("Header1"))
	assert.Equal(t, "Value2", resp.Header.Get("Header2"))
}

func getResponseBody(t *testing.T, resp *http.Response) string {
	data, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	require.Nil(t, err)
	return string(data)
}
