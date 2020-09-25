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
