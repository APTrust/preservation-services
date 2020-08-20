// +build !partners

package network_test

import (
	"fmt"
	"github.com/APTrust/preservation-services/network"
	"github.com/APTrust/preservation-services/util"
	"github.com/APTrust/preservation-services/util/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"
	"time"
)

var port = 8818
var serviceUrl = fmt.Sprintf("http://127.0.0.1:%d", port)
var volumeService *network.VolumeService

func runService(t *testing.T) {
	if volumeService == nil {
		log := logger.DiscardLogger("test_volume_service")
		volumeService = network.NewVolumeService(port, log)
		require.NotNil(t, volumeService)
		go volumeService.Serve()
		time.Sleep(500 * time.Millisecond)
	}
}

func TestNewVolumeService(t *testing.T) {
	if util.RunningInCI() {
		t.Skip("Skipping volume service test because it looks like we're in the CI environment.")
	}
	runService(t)
}

func TestReserve(t *testing.T) {
	if util.RunningInCI() {
		t.Skip("Skipping volume service test because it looks like we're in the CI environment.")
	}
	runService(t)

	reserveUrl := fmt.Sprintf("%s/reserve/", serviceUrl)

	// Start with a good request
	params := url.Values{
		"path":  {"/tmp/some_file"},
		"bytes": {"8000"},
	}
	resp, err := http.PostForm(reserveUrl, params)
	require.Nil(t, err)
	data, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	resp.Body.Close()

	expected := `{"Succeeded":true,"ErrorMessage":"","Data":null}`
	assert.Equal(t, expected, string(data))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Bad request: no path
	params = url.Values{
		"bytes": {"8000"},
	}
	resp, err = http.PostForm(reserveUrl, params)
	require.Nil(t, err)
	data, err = ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	resp.Body.Close()

	expected = `{"Succeeded":false,"ErrorMessage":"Param 'path' is required.","Data":null}`
	assert.Equal(t, expected, string(data))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// Bad request: no value for bytes
	params = url.Values{
		"path": {"/tmp/some_file"},
	}
	resp, err = http.PostForm(reserveUrl, params)
	require.Nil(t, err)
	data, err = ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	resp.Body.Close()

	expected = `{"Succeeded":false,"ErrorMessage":"Param 'bytes' must be an integer greater than zero.","Data":null}`
	assert.Equal(t, expected, string(data))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestRelease(t *testing.T) {
	if util.RunningInCI() {
		t.Skip("Skipping volume service test because it looks like we're in the CI environment.")
	}
	runService(t)

	reserveUrl := fmt.Sprintf("%s/release/", serviceUrl)

	// Good request
	params := url.Values{
		"path": {"/tmp/some_file"},
	}
	resp, err := http.PostForm(reserveUrl, params)
	require.Nil(t, err)
	data, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	resp.Body.Close()

	expected := `{"Succeeded":true,"ErrorMessage":"","Data":null}`
	assert.Equal(t, expected, string(data))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Bad request - no path
	params = url.Values{
		"useless_param": {"/tmp/some_file"},
	}
	resp, err = http.PostForm(reserveUrl, params)
	require.Nil(t, err)
	data, err = ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	resp.Body.Close()

	expected = `{"Succeeded":false,"ErrorMessage":"Param 'path' is required.","Data":null}`
	assert.Equal(t, expected, string(data))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestReport(t *testing.T) {
	if util.RunningInCI() {
		t.Skip("Skipping volume service test because it looks like we're in the CI environment.")
	}
	runService(t)

	// Reserve a chunk of space with 8000 bytes
	reserveUrl := fmt.Sprintf("%s/reserve/", serviceUrl)
	params := url.Values{
		"path":  {"/tmp/some_file"},
		"bytes": {"8000"},
	}
	resp, err := http.PostForm(reserveUrl, params)
	require.Nil(t, err)
	data, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Reserve another chunk with 24000 bytes
	params = url.Values{
		"path":  {"/tmp/some_other_file"},
		"bytes": {"24000"},
	}
	resp, err = http.PostForm(reserveUrl, params)
	require.Nil(t, err)
	data, err = ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	reportUrl := fmt.Sprintf("%s/report/", serviceUrl)
	resp, err = http.Get(reportUrl)
	require.Nil(t, err)
	data, err = ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	resp.Body.Close()

	expected := `{"Succeeded":false,"ErrorMessage":"Param 'path' is required.","Data":null}`
	assert.Equal(t, expected, string(data))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	reportUrl = fmt.Sprintf("%s/report/?path=/", serviceUrl)
	resp, err = http.Get(reportUrl)
	require.Nil(t, err)
	data, err = ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	resp.Body.Close()

	expected = `{"Succeeded":true,"ErrorMessage":"","Data":{"/tmp/some_file":8000,"/tmp/some_other_file":24000}}`
	assert.Equal(t, expected, string(data))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestPing(t *testing.T) {
	if util.RunningInCI() {
		t.Skip("Skipping volume service test because it looks like we're in the CI environment.")
	}
	runService(t)

	pingUrl := fmt.Sprintf("%s/ping/", serviceUrl)
	resp, err := http.Get(pingUrl)
	require.Nil(t, err)
	data, err := ioutil.ReadAll(resp.Body)
	assert.Nil(t, err)
	resp.Body.Close()

	expected := `{"Succeeded":true,"ErrorMessage":"","Data":null}`
	assert.Equal(t, expected, string(data))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
