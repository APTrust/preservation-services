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
		Stage:          constants.StageRequested,
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
		Action:         constants.ActionRestore,
		Stage:          "",
		FileIdentifier: "",
		Expected:       constants.TopicObjectRestore,
	},
	Item{
		Action:         constants.ActionRestore,
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
