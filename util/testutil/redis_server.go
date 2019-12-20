package testutil

import (
	"github.com/alicebob/miniredis/v2"
)

type RedisServer struct {
	server *miniredis.Miniredis
}

func NewRedisServer() *RedisServer {
	var err error
	server, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	return &RedisServer{
		server: server,
	}
}

func (s *RedisServer) Addr() string {
	return s.server.Addr()
}

func (s *RedisServer) Close() {
	s.server.Close()
}
