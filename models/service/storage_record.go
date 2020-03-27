package service

import (
	"time"
)

type StorageRecord struct {
	Bucket     string    `json:"bucket"`
	ETag       string    `json:"etag"`
	Provider   string    `json:"provider"`
	Size       int64     `json:"size"`
	StoredAt   time.Time `json:"stored_at"`
	URL        string    `json:"url"`
	VerifiedAt time.Time `json:"verified_at"`
}
