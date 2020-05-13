package deletion_test

import (
	"testing"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/deletion"
	"github.com/APTrust/preservation-services/models/common"
	//"github.com/APTrust/preservation-services/models/service"
	"github.com/stretchr/testify/assert"
	//"github.com/stretchr/testify/require"
)

func TestNewManager(t *testing.T) {
	context := common.NewContext()
	manager := deletion.NewManager(
		context,
		9999,
		"test.edu/my_object",
		constants.TypeObject,
	)
	assert.NotNil(t, manager)
	assert.Equal(t, context, manager.Context)
	assert.Equal(t, 9999, manager.WorkItemID)
	assert.Equal(t, "test.edu/my_object", manager.Identifier)
	assert.Equal(t, constants.TypeObject, manager.ItemType)
}

func TestRun_SingleFile(t *testing.T) {

}

func TestRun_Object(t *testing.T) {

}

func prepareSingleFileTest(t *testing.T, context *common.Context) {

}

func prepareObjectTest(t *testing.T, context *common.Context) {

}
