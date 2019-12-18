package service

import (
	"time"
)

type StorageRecord struct {
	URL      string    `json:"url"`
	StoredAt time.Time `json:"stored_at"`
}
