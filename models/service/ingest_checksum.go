package service

import (
	"github.com/APTrust/preservation-services/models/registry"
	"time"
)

type IngestChecksum struct {
	Algorithm string    `json:"algorithm"`
	DateTime  time.Time `json:"datetime"`
	Digest    string    `json:"digest"`
	Source    string    `json:"source"`
}

func (cs *IngestChecksum) ToRegistryChecksum(genericFileId int) *registry.Checksum {
	return &registry.Checksum{
		Algorithm:     cs.Algorithm,
		DateTime:      cs.DateTime,
		Digest:        cs.Digest,
		GenericFileId: genericFileId,
	}
}
