package ingest

import (
	"time"
)

type ChecksumSource string

const (
	Ingest   ChecksumSource = "ingest"
	Manifest ChecksumSource = "manifest"
	Registry ChecksumSource = "registry"
)

type IngestChecksum struct {
	Source    ChecksumSource `json:"source"`
	Algorithm string         `json:"algorithm"`
	DateTime  time.Time      `json:"datetime"`
	Digest    string         `json:"digest"`
}
