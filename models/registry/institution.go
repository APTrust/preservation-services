package registry

import (
	"encoding/json"
	"time"
)

type Institution struct {
	CreatedAt           time.Time `json:"created_at"`
	DeactivatedAt       time.Time `json:"deactivated_at,omitempty"`
	ID                  int64     `json:"id"`
	Identifier          string    `json:"identifier"`
	MemberInstitutionID int64     `json:"member_institution_id"`
	Name                string    `json:"name"`
	OTPEnabled          bool      `json:"otp_enabled"`
	ReceivingBucket     string    `json:"receiving_bucket"`
	RestoreBucket       string    `json:"restore_bucket"`
	State               string    `json:"state"`
	Type                string    `json:"type"`
	UpdatedAt           time.Time `json:"updated_at"`
}

func InstitutionFromJSON(jsonData []byte) (*Institution, error) {
	inst := &Institution{}
	err := json.Unmarshal(jsonData, inst)
	if err != nil {
		return nil, err
	}
	return inst, nil
}

func (inst *Institution) ToJSON() ([]byte, error) {
	bytes, err := json.Marshal(inst)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// JSON format for Pharos post/put is {"institution": <object>}
func (inst *Institution) SerializeForPharos() ([]byte, error) {
	dataStruct := make(map[string]*Institution)
	dataStruct["institution"] = inst
	return json.Marshal(dataStruct)
}
