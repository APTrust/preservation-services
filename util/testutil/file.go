package testutil

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
)

var TempDir, _ = ioutil.TempDir("", "prod-serv-test")

func ProjectRoot() string {
	_, thisFile, _, _ := runtime.Caller(0)
	absPath, _ := filepath.Abs(path.Join(thisFile, "..", "..", ".."))
	return absPath
}

func PathToTestData() string {
	return path.Join(ProjectRoot(), "testdata")
}

func PathToUnitTestBags() string {
	return path.Join(ProjectRoot(), "testdata", "unit_test_bags")
}

func PathToPharosFixture(filename string) string {
	return path.Join(PathToTestData(), "pharos", filename)
}

func PathToUnitTestBag(filename string) string {
	return path.Join(PathToUnitTestBags(), filename)
}

func ReadPharosFixture(filename string) ([]byte, error) {
	file, err := os.Open(PathToPharosFixture(filename))
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return ioutil.ReadAll(file)
}
