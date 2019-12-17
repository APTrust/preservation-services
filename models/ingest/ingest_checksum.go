package ingest

import (
	"time"
)

type IngestChecksum struct {
	Algorithm string    `json:"algorithm"`
	DateTime  time.Time `json:"datetime"`
	Digest    string    `json:"digest"`
	Source    string    `json:"source"`
}
