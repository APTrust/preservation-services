package registry

import (
	"time"
)

// InstitutionView is a read-only object containing information about
// an institution and its parent (if it has one).
type InstitutionView struct {
	ID                  int64     `json:"id"`
	Name                string    `json:"name"`
	Identifier          string    `json:"identifier"`
	State               string    `json:"state"`
	Type                string    `json:"type"`
	DeactivatedAt       time.Time `json:"deactivated_at"`
	OTPEnabled          bool      `json:"otp_enabled"`
	EnableSpotRestore   bool      `json:"enable_spot_restore"`
	ReceivingBucket     string    `json:"receiving_bucket"`
	RestoreBucket       string    `json:"restore_bucket"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
	ParentId            int64     `json:"parent_id"`
	ParentName          string    `json:"parent_name"`
	ParentIdentifier    string    `json:"parent_identifier"`
	ParentState         string    `json:"parent_state"`
	ParentDeactivatedAt time.Time `json:"parent_deactivated_at"`
}
