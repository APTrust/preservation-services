package constants_test

import (
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Item struct {
	Action         string
	Stage          string
	FileIdentifier string
	Expected       string
}

var items = []Item{
	Item{
		Action:         constants.ActionIngest,
		Stage:          constants.StageReceive,
		FileIdentifier: "",
		Expected:       constants.IngestPreFetch,
	},
	Item{
		Action:         constants.ActionIngest,
		Stage:          constants.StageValidate,
		FileIdentifier: "",
		Expected:       constants.IngestValidation,
	},
	Item{
		Action:         constants.ActionIngest,
		Stage:          constants.StageReingestCheck,
		FileIdentifier: "",
		Expected:       constants.IngestReingestCheck,
	},
	Item{
		Action:         constants.ActionIngest,
		Stage:          constants.StageFormatIdentification,
		FileIdentifier: "",
		Expected:       constants.IngestFormatIdentification,
	},
	Item{
		Action:         constants.ActionIngest,
		Stage:          constants.StageStore,
		FileIdentifier: "",
		Expected:       constants.IngestStorage,
	},
	Item{
		Action:         constants.ActionIngest,
		Stage:          constants.StageStorageValidation,
		FileIdentifier: "",
		Expected:       constants.IngestStorageValidation,
	},
	Item{
		Action:         constants.ActionIngest,
		Stage:          constants.StageRecord,
		FileIdentifier: "",
		Expected:       constants.IngestRecord,
	},
	Item{
		Action:         constants.ActionIngest,
		Stage:          constants.StageCleanup,
		FileIdentifier: "",
		Expected:       constants.IngestCleanup,
	},
	Item{
		Action:         constants.ActionFixityCheck,
		Stage:          "",
		FileIdentifier: "",
		Expected:       constants.TopicFixity,
	},
	Item{
		Action:         constants.ActionRestoreObject,
		Stage:          "",
		FileIdentifier: "",
		Expected:       constants.TopicObjectRestore,
	},
	Item{
		Action:         constants.ActionRestoreFile,
		Stage:          "",
		FileIdentifier: "test.edu/bag/data/file.txt",
		Expected:       constants.TopicFileRestore,
	},
	Item{
		Action:         constants.ActionGlacierRestore,
		Stage:          "",
		FileIdentifier: "",
		Expected:       constants.TopicGlacierRestore,
	},
	Item{
		Action:         constants.ActionDelete,
		Stage:          "",
		FileIdentifier: "",
		Expected:       constants.TopicDelete,
	},
	Item{
		Action:         constants.ActionDelete,
		Stage:          "",
		FileIdentifier: "test.edu/bag/data/file.txt",
		Expected:       constants.TopicDelete,
	},
}

func TestTopicFor(t *testing.T) {
	for _, item := range items {
		topic, err := constants.TopicFor(item.Action, item.Stage, item.FileIdentifier)
		require.Nil(t, err)
		assert.Equal(t, item.Expected, topic, "For %s/%s", item.Action, item.Stage)
	}
}

func TestIngestStageFor(t *testing.T) {
	stage, err := constants.IngestStageFor(constants.IngestPreFetch)
	assert.Nil(t, err)
	assert.Equal(t, constants.StageReceive, stage)

	stage, err = constants.IngestStageFor(constants.IngestValidation)
	assert.Nil(t, err)
	assert.Equal(t, constants.StageValidate, stage)

	stage, err = constants.IngestStageFor(constants.IngestReingestCheck)
	assert.Nil(t, err)
	assert.Equal(t, constants.StageReingestCheck, stage)

	stage, err = constants.IngestStageFor(constants.IngestStaging)
	assert.Nil(t, err)
	assert.Equal(t, constants.StageCopyToStaging, stage)

	stage, err = constants.IngestStageFor(constants.IngestFormatIdentification)
	assert.Nil(t, err)
	assert.Equal(t, constants.StageFormatIdentification, stage)

	stage, err = constants.IngestStageFor(constants.IngestStorage)
	assert.Nil(t, err)
	assert.Equal(t, constants.StageStore, stage)

	stage, err = constants.IngestStageFor(constants.IngestStorageValidation)
	assert.Nil(t, err)
	assert.Equal(t, constants.StageStorageValidation, stage)

	stage, err = constants.IngestStageFor(constants.IngestRecord)
	assert.Nil(t, err)
	assert.Equal(t, constants.StageRecord, stage)

	stage, err = constants.IngestStageFor(constants.IngestCleanup)
	assert.Nil(t, err)
	assert.Equal(t, constants.StageCleanup, stage)
}
