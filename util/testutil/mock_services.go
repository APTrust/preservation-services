package testutil

import (
	"io"
	"net/http"
	"os"
)

// These functions allow us to mock http responses from S3, Redis, and Pharos.
//
// TODO: Http passthrough set and get: pass data through to Redis so
// we can examine it later in tests.
//
// HttpPassThroughSetter(headers map[string]string, setUUID, data, getUUID string)
//    sets value data with key setUUID in redis,
//    and returns value getUUID from redis
//
// HttpPassThroughGetter(headers map[string]string, uuid string)
//    returns value of key uuid from redis

var EmptyHeaders = make(map[string]string, 0)

// Returns an http handler function that returns the contents
// of the specified file, along with the specified headers.
func HttpFileResponder(headers map[string]string, filePath string) http.HandlerFunc {
	f := func(w http.ResponseWriter, r *http.Request) {
		setHeaders(w, headers)
		f, err := os.Open(filePath)
		if err != nil {
			panic(err)
		}
		io.Copy(w, f)
	}
	return http.HandlerFunc(f)
}

// Returns an http handler function that returns the specified
// string, along with the specified headers.
func HttpStringResponder(headers map[string]string, data string) http.HandlerFunc {
	f := func(w http.ResponseWriter, r *http.Request) {
		setHeaders(w, headers)
		w.Write([]byte(data))
	}
	return http.HandlerFunc(f)
}

func setHeaders(w http.ResponseWriter, headers map[string]string) {
	if headers != nil {
		for key, value := range headers {
			w.Header().Set(key, value)
		}
	}
}
