package testutil

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/APTrust/preservation-services/util"
)

var TempDir, _ = ioutil.TempDir("", "prod-serv-test")

func PathToTestData() string {
	return path.Join(util.ProjectRoot(), "testdata")
}

func PathToUnitTestBags() string {
	return path.Join(util.ProjectRoot(), "testdata", "unit_test_bags")
}

func PathToIntTestBags() string {
	return path.Join(util.ProjectRoot(), "testdata", "int_test_bags")
}

func PathToE2EFile(filename string) string {
	return path.Join(util.ProjectRoot(), "testdata", "e2e_results", filename)
}

func PathToRegistryFixture(filename string) string {
	return path.Join(PathToTestData(), "registry", filename)
}

func PathToUnitTestBag(filename string) string {
	return path.Join(PathToUnitTestBags(), filename)
}

func ReadRegistryFixture(filename string) ([]byte, error) {
	return ReadFile(PathToRegistryFixture(filename))
}

func ReadE2EFile(filename string) ([]byte, error) {
	return ReadFile(PathToE2EFile(filename))
}

func ReadFile(filepath string) ([]byte, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return ioutil.ReadAll(file)
}
