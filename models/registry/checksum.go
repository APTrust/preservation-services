package registry

import (
	"time"
)

type Checksum struct {
	Id            int       `json:"id,omitempty"`
	GenericFileId int       `json:"generic_file_id"`
	Algorithm     string    `json:"algorithm"`
	DateTime      time.Time `json:"datetime"`
	Digest        string    `json:"digest"`
	CreatedAt     time.Time `json:"created_at,omitempty"`
	UpdatedAt     time.Time `json:"updated_at,omitempty"`
}
