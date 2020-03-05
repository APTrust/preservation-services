// +build integration

package ingest_test

import (
	//	"fmt"
	//	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	//	"github.com/APTrust/preservation-services/models/service"
	//	"github.com/APTrust/preservation-services/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	//	"os"
	//	"path"
	//	"strings"
	"testing"
)

// Existing ids, loaded in Pharos integration test fixture data
const ObjIdExists = "institution2.edu/toads"
const FileIdExists = "institution2.edu/coal/doc3"

func SetupReingest(t *testing.T) {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	g := ingest.NewMetadataGatherer(context, 9999, obj)
	err := g.ScanBag()
	require.Nil(t, err)
}

func GetReingestManager() *ingest.ReingestManager {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	return ingest.NewReingestManager(context, 9999, obj)
}

func TestNewReingestManager(t *testing.T) {
	manager := GetReingestManager()
	assert.NotNil(t, manager.Context)
	assert.Equal(t, "example.edu.tagsample_good.tar", manager.IngestObject.S3Key)
	assert.Equal(t, 9999, manager.WorkItemId)
}

func TestObjectWasPreviouslyIngested(t *testing.T) {
	manager := GetReingestManager()

	// Set these properties on our test object so that
	// IngestObject.Indentifier() resolves to
	// "institution2.edu/toads", which does exist as part
	// of the Pharos fixture data.
	manager.IngestObject.S3Key = "toads.tar"
	manager.IngestObject.Institution = "institution2.edu"
	wasIngested, err := manager.ObjectWasPreviouslyIngested()
	require.Nil(t, err)
	assert.True(t, wasIngested)

	// Set this so Identifier() resolves to
	// "institution.edu/bag-does-not-exist"
	manager.IngestObject.S3Key = "bag-does-not-exist.tar"
	wasIngested, err = manager.ObjectWasPreviouslyIngested()
	require.Nil(t, err)
	assert.False(t, wasIngested)
}
