package registry_test

import (
	//"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

var inst = &registry.Institution{
	CreatedAt:           testutil.Bloomsday,
	DeactivatedAt:       testutil.Bloomsday,
	Id:                  999,
	Identifier:          "hardknocks.edu",
	MemberInstitutionId: 999,
	Name:                "School of Hard Knocks",
	OTPEnabled:          false,
	ReceivingBucket:     "aptrust-hk-receiving",
	RestoreBucket:       "aptrust-hk-restore",
	State:               "A",
	Type:                "",
	UpdatedAt:           testutil.Bloomsday,
}

var instJson = `{"created_at":"1904-06-16T15:04:05Z","deactivated_at":"1904-06-16T15:04:05Z","id":999,"identifier":"hardknocks.edu","member_institution_id":999,"name":"School of Hard Knocks","otp_enabled":false,"receiving_bucket":"aptrust-hk-receiving","restore_bucket":"aptrust-hk-restore","state":"A","type":"","updated_at":"1904-06-16T15:04:05Z"}`

func TestInstitutionFromJson(t *testing.T) {
	institution, err := registry.InstitutionFromJson([]byte(instJson))
	require.Nil(t, err)
	assert.Equal(t, inst, institution)
}

func TestInstitutionToJson(t *testing.T) {
	actualJson, err := inst.ToJson()
	require.Nil(t, err)
	assert.Equal(t, instJson, string(actualJson))
}
