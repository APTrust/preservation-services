package util

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	ps "github.com/mitchellh/go-ps"
)

// IsRunningInOtherProcess returns true if the pid file at pathToFile
// contains a pid belonging to another process.
func IsRunningInOtherProcess(pathToFile string) bool {
	if FileExists(pathToFile) {
		pid := ReadPidFile(pathToFile)
		return pid != 0 && pid != os.Getpid()
	}
	return false
}

// ReadPidFile returns the pid from the speficied file.
func ReadPidFile(pathToFile string) int {
	if data, err := ioutil.ReadFile(pathToFile); err == nil {
		if pid, err := strconv.Atoi(string(data)); err == nil {
			return pid
		}
	}
	return 0
}

// WritePidFile writes this process' pid to the specified file.
func WritePidFile(pathToFile string) error {
	pidStr := strconv.Itoa(os.Getpid())
	return ioutil.WriteFile(pathToFile, []byte(pidStr), 0664)
}

// DeletePidFile deletes the specified pid file, if it looks safe to delete.
func DeletePidFile(pathToFile string) error {
	if LooksSafeToDelete(pathToFile, 12, 2) {
		return os.Remove(pathToFile)
	}
	return fmt.Errorf("Pid file %s does not look safe to delete", pathToFile)
}

// AgeOfPidFile returns the duration of time that has passed since
// the pid file was last modified.
func AgeOfPidFile(pathToFile string) (time.Duration, error) {
	zeroDuration, _ := time.ParseDuration("0s")
	fileStat, err := os.Stat(pathToFile)
	if err != nil {
		return zeroDuration, err
	}
	return time.Since(fileStat.ModTime()), nil
}

// ProcessIsRunning returns true if the process with pid is running.
// This uses go-ps internally because golang's os.FindProcess always
// returns a process on *nix, even when no process with that pid is
// running.
func ProcessIsRunning(pid int) bool {
	proc, _ := ps.FindProcess(pid)
	return proc != nil
}
