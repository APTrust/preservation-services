package registry

import (
	"encoding/json"
	"time"
)

type Institution struct {
	Id                  int       `json:"id"`
	Name                string    `json:"name"`
	Identifier          string    `json:"identifier"`
	State               string    `json:"state"`
	Type                string    `json:"type"`
	MemberInstitutionId int       `json:"member_institution_id"`
	ReceivingBucket     string    `json:"receiving_bucket"`
	RestoreBucket       string    `json:"restore_bucket"`
	OTPEnabled          bool      `json:"otp_enabled"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
	DeactivatedAt       time.Time `json:"deactivated_at,omitempty"`
}

func InstitutionFromJson(jsonData string) (*Institution, error) {
	inst := &Institution{}
	err := json.Unmarshal([]byte(jsonData), inst)
	if err != nil {
		return nil, err
	}
	return inst, nil
}

func (inst *Institution) ToJson() (string, error) {
	bytes, err := json.Marshal(inst)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
