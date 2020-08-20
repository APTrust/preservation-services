// +build !partners

// Don't include this in partner apps because it's not needed. Also,
// the syscall.Stat* functions inside common.Volume don't work on Windows.
package network

import (
	"encoding/json"
	"fmt"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/platform"
	"github.com/op/go-logging"
	"net/http"
	"strconv"
)

// VolumeService keeps track of the space available to workers
// processing APTrust bags.
type VolumeService struct {
	port    int
	volumes map[string]*service.Volume
	logger  *logging.Logger
}

// NewVolumeService creates a new VolumeService object to track the
// amount of available space and claimed space on locally mounted
// volumes.
func NewVolumeService(port int, logger *logging.Logger) *VolumeService {
	return &VolumeService{
		port:    port,
		volumes: make(map[string]*service.Volume),
		logger:  logger,
	}
}

// Serve starts an HTTP server, so the VolumeService can respond to
// requests from the VolumeClient(s). See the VolumeClient for available
// calls.
func (vs *VolumeService) Serve() {
	http.HandleFunc("/reserve/", vs.makeReserveHandler())
	http.HandleFunc("/release/", vs.makeReleaseHandler())
	http.HandleFunc("/report/", vs.makeReportHandler())
	http.HandleFunc("/ping/", vs.makePingHandler())
	listenAddr := fmt.Sprintf("127.0.0.1:%d", vs.port)
	http.ListenAndServe(listenAddr, nil)
}

// Returns a Volume object with info about the volume at the specified
// mount point. The mount point should be the path to a disk or partition.
// For example, "/", "/mnt/data", etc.
func (vs *VolumeService) getVolume(path string) *service.Volume {
	mountpoint, err := platform.GetMountPointFromPath(path)
	if err != nil {
		mountpoint = "/"
		vs.logger.Error("Cannot determine mountpoint of file '%s': %v",
			path, err)
	}
	if _, keyExists := vs.volumes[mountpoint]; !keyExists {
		vs.volumes[mountpoint] = service.NewVolume(mountpoint)
	}
	return vs.volumes[mountpoint]
}

func (vs *VolumeService) makeReserveHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := &service.VolumeResponse{}
		status := http.StatusOK
		path := r.FormValue("path")
		bytes, err := strconv.ParseUint(r.FormValue("bytes"), 10, 64)
		if path == "" {
			response.Succeeded = false
			response.ErrorMessage = "Param 'path' is required."
			status = http.StatusBadRequest
		} else if err != nil || bytes < 1 {
			response.Succeeded = false
			response.ErrorMessage = "Param 'bytes' must be an integer greater than zero."
			status = http.StatusBadRequest
		} else {
			volume := vs.getVolume(path)
			err = volume.Reserve(path, bytes)
			if err != nil {
				response.Succeeded = false
				response.ErrorMessage = fmt.Sprintf(
					"Could not reserve %d bytes for file '%s': %v",
					bytes, path, err)
				vs.logger.Error("[%s] %s", r.RemoteAddr, response.ErrorMessage)
				status = http.StatusInternalServerError
			} else {
				response.Succeeded = true
				vs.logger.Info("[%s] Reserved %d bytes for %s", r.RemoteAddr, bytes, path)
			}
		}
		jsonResponse, _ := json.Marshal(response)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(status)
		w.Write(jsonResponse)
	}
}

func (vs *VolumeService) makeReleaseHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := &service.VolumeResponse{}
		path := r.FormValue("path")
		status := http.StatusOK
		if path == "" {
			response.Succeeded = false
			response.ErrorMessage = "Param 'path' is required."
			status = http.StatusBadRequest
		} else {
			volume := vs.getVolume(path)
			volume.Release(path)
			response.Succeeded = true
			vs.logger.Info("[%s] Released %s", r.RemoteAddr, path)
		}
		jsonResponse, _ := json.Marshal(response)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(status)
		w.Write(jsonResponse)
	}
}

func (vs *VolumeService) makeReportHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := &service.VolumeResponse{}
		path := r.FormValue("path")
		status := http.StatusOK
		if path == "" {
			response.Succeeded = false
			response.ErrorMessage = "Param 'path' is required."
			status = http.StatusBadRequest
		} else {
			volume := vs.getVolume(path)
			response.Succeeded = true
			response.Data = volume.Reservations()
			vs.logger.Info("[%s] Reservations (%d)", r.RemoteAddr, path, len(response.Data))
		}
		jsonResponse, _ := json.Marshal(response)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(status)
		w.Write(jsonResponse)
	}
}

func (vs *VolumeService) makePingHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := &service.VolumeResponse{}
		response.Succeeded = true
		status := http.StatusOK
		jsonResponse, _ := json.Marshal(response)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(status)
		w.Write(jsonResponse)
	}
}
