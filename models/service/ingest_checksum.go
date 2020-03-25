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

func (cs *IngestChecksum) ToRegistryChecksum(genericFileID int) *registry.Checksum {
	return &registry.Checksum{
		Algorithm:     cs.Algorithm,
		DateTime:      cs.DateTime,
		Digest:        cs.Digest,
		GenericFileID: genericFileID,
	}
}
