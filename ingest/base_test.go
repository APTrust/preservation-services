package ingest_test

import (
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getIngestBase() *ingest.Base {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	return &ingest.Base{
		Context:      context,
		WorkItemID:   3349,
		IngestObject: obj,
	}
}

func TestIngestBase_ObjectSave(t *testing.T) {
	ingestBase := getIngestBase()
	err := ingestBase.IngestObjectSave()
	require.Nil(t, err)

	client := ingestBase.Context.RedisClient
	objIdentifier := ingestBase.IngestObject.Identifier()
	obj, err := client.IngestObjectGet(ingestBase.WorkItemID, objIdentifier)
	require.Nil(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, objIdentifier, obj.Identifier())
}

func TestIngestBase_FileSaveAndGet(t *testing.T) {
	ingestBase := getIngestBase()
	ingestFile := &service.IngestFile{
		ObjectIdentifier: ingestBase.IngestObject.Identifier(),
		PathInBag:        "data/images/photo.jpg",
		UUID:             constants.EmptyUUID,
	}
	err := ingestBase.IngestFileSave(ingestFile)
	require.Nil(t, err)

	savedFile, err := ingestBase.IngestFileGet(ingestFile.Identifier())
	require.Nil(t, err)
	require.NotNil(t, savedFile)
	assert.Equal(t, ingestFile.ObjectIdentifier, savedFile.ObjectIdentifier)
	assert.Equal(t, ingestFile.UUID, savedFile.UUID)
}
