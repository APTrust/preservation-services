package testutil

import (
	"path"
	"path/filepath"
	"runtime"
)

func ProjectRoot() string {
	_, thisFile, _, _ := runtime.Caller(1)
	absPath, _ := filepath.Abs(path.Join(thisFile, "..", ".."))
	return absPath
}

func PathToTestData() string {
	return path.Join(ProjectRoot(), "testdata")
}

func PathToUnitTestBags() string {
	return path.Join(ProjectRoot(), "testdata", "unit_test_bags")
}

func PathToUnitTestBag(filename string) string {
	return path.Join(PathToUnitTestBags(), filename)
}
