package services_test

import (
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
	"github.com/stretchr/testify/assert"
	"path"
	"runtime"
	"testing"
)

func getTarFileReader(t *testing.T, filename string) io.Reader {
	pathToFile := util.PathToUnitTestBag(filename)
	reader, err := os.Open(pathToFile)
	require.Nil(t, err)
	return reader
}

func TestNewTarredBagScanner(t *testing.T) {
	reader := getTarFileReader("example.edu.sample_good.tar")
	defer reader.Close()

}
