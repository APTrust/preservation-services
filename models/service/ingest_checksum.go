package service

import (
	"time"

	"github.com/APTrust/preservation-services/models/registry"
)

type IngestChecksum struct {
	Algorithm string    `json:"algorithm"`
	DateTime  time.Time `json:"datetime"`
	Digest    string    `json:"digest"`
	Source    string    `json:"source"`
}

// ToRegistryChecksum converts this checksum to a registry.Checksum,
// which is the format used in Pharos.
func (cs *IngestChecksum) ToRegistryChecksum(genericFileID int) *registry.Checksum {
	return &registry.Checksum{
		Algorithm:     cs.Algorithm,
		DateTime:      cs.DateTime,
		Digest:        cs.Digest,
		GenericFileID: genericFileID,
		CreatedAt:     cs.DateTime,
		UpdatedAt:     cs.DateTime,
	}
}
