package testutil

import (
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"net/http/httptest"
)

const ReceivingBucket = "receiving"
const StagingBucket = "staging"
const PreservationBucket = "preservation"
const ReplicationBucket = "replication"

type S3Server struct {
	server *httptest.Server
	URL    string
}

// NewS3Server creates a new S3 server that stores objects in memory.
// This is used only for automated testing.
func NewS3Server() *S3Server {
	backend := s3mem.New()
	backend.CreateBucket(ReceivingBucket)
	backend.CreateBucket(StagingBucket)
	backend.CreateBucket(PreservationBucket)
	backend.CreateBucket(ReplicationBucket)
	faker := gofakes3.New(backend)
	server := httptest.NewServer(faker.Server())
	return &S3Server{
		server: server,
		URL:    server.URL,
	}
}

// Close shuts down the S3 test server.
func (s *S3Server) Close() {
	s.server.Close()
}
