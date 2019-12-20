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

func (s *S3Server) Close() {
	s.server.Close()
}
