package registry

import (
	"encoding/json"
	"time"
)

type Institution struct {
	CreatedAt           time.Time `json:"created_at"`
	DeactivatedAt       time.Time `json:"deactivated_at,omitempty"`
	Id                  int       `json:"id"`
	Identifier          string    `json:"identifier"`
	MemberInstitutionId int       `json:"member_institution_id"`
	Name                string    `json:"name"`
	OTPEnabled          bool      `json:"otp_enabled"`
	ReceivingBucket     string    `json:"receiving_bucket"`
	RestoreBucket       string    `json:"restore_bucket"`
	State               string    `json:"state"`
	Type                string    `json:"type"`
	UpdatedAt           time.Time `json:"updated_at"`
}

func InstitutionFromJson(jsonData []byte) (*Institution, error) {
	inst := &Institution{}
	err := json.Unmarshal(jsonData, inst)
	if err != nil {
		return nil, err
	}
	return inst, nil
}

func (inst *Institution) ToJson() ([]byte, error) {
	bytes, err := json.Marshal(inst)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}
