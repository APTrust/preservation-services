package ingest_test

import (
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/ingest"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func getIngestWorker() *ingest.IngestWorker {
	context := common.NewContext()
	obj := getIngestObject(pathToGoodBag, goodbagMd5)
	return &ingest.IngestWorker{
		Context:      context,
		WorkItemId:   3349,
		IngestObject: obj,
	}
}

func TestIngestWorker_ObjectSave(t *testing.T) {
	ingestWorker := getIngestWorker()
	err := ingestWorker.IngestObjectSave()
	require.Nil(t, err)

	client := ingestWorker.Context.RedisClient
	objIdentifier := ingestWorker.IngestObject.Identifier()
	obj, err := client.IngestObjectGet(ingestWorker.WorkItemId, objIdentifier)
	require.Nil(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, objIdentifier, obj.Identifier())
}

func TestIngestWorker_FileSaveAndGet(t *testing.T) {
	ingestWorker := getIngestWorker()
	ingestFile := &service.IngestFile{
		ObjectIdentifier: ingestWorker.IngestObject.Identifier(),
		PathInBag:        "data/images/photo.jpg",
		UUID:             constants.EmptyUUID,
	}
	err := ingestWorker.IngestFileSave(ingestFile)
	require.Nil(t, err)

	savedFile, err := ingestWorker.IngestFileGet(ingestFile.Identifier())
	require.Nil(t, err)
	require.NotNil(t, savedFile)
	assert.Equal(t, ingestFile.ObjectIdentifier, savedFile.ObjectIdentifier)
	assert.Equal(t, ingestFile.UUID, savedFile.UUID)
}
