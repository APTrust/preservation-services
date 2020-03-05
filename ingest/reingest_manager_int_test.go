// +build integration

package ingest_test

import (
	"fmt"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
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

func TestSetStorageOption(t *testing.T) {
	genericFile := &registry.GenericFile{
		State:         "D",
		StorageOption: constants.StorageGlacierOH,
	}
	ingestFile := &service.IngestFile{
		StorageOption: constants.StorageStandard,
	}

	// StorageOption should NOT change if GenericFile is deleted.
	manager := GetReingestManager()
	manager.SetStorageOption(ingestFile, genericFile)
	assert.Equal(t, constants.StorageStandard, ingestFile.StorageOption)

	// StorageOption should change if GenericFile is active.
	genericFile.State = "A"
	manager.SetStorageOption(ingestFile, genericFile)
	assert.Equal(t, constants.StorageGlacierOH, ingestFile.StorageOption)
}

func TestFlagForUpdate(t *testing.T) {
	pharosUUID := "c445c30b-2299-4796-b803-e3c6ee43a2ae"
	genericFile := &registry.GenericFile{
		URI: fmt.Sprintf("https://example.com/storage/%s", pharosUUID),
	}
	ingestFile := &service.IngestFile{
		NeedsSave: false,
		UUID:      "c4ddee73-cbae-4f4e-a93b-ffcb0a0f2e99",
	}
	manager := GetReingestManager()
	manager.FlagForUpdate(ingestFile, genericFile)
	assert.True(t, ingestFile.NeedsSave)
	assert.Equal(t, pharosUUID, ingestFile.UUID)
}

func TestFlagUnchanged(t *testing.T) {
	pharosUUID := "c445c30b-2299-4796-b803-e3c6ee43a2ae"
	genericFile := &registry.GenericFile{
		URI: fmt.Sprintf("https://example.com/storage/%s", pharosUUID),
	}
	ingestFile := &service.IngestFile{
		NeedsSave: true,
		UUID:      "c4ddee73-cbae-4f4e-a93b-ffcb0a0f2e99",
	}
	manager := GetReingestManager()
	manager.FlagUnchanged(ingestFile, genericFile)
	assert.False(t, ingestFile.NeedsSave)
	assert.Equal(t, pharosUUID, ingestFile.UUID)
}
