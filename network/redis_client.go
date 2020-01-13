package network

import (
	"fmt"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/go-redis/redis/v7"
	"strconv"
)

// RedisClient is a client that lets workers store and retrieve working
// data from a Redis server.
type RedisClient struct {
	client *redis.Client
}

// NewRedisClient creates a new RedisClient. Param address is the net address
// of the Redis server. Param password is the password required to connect.
// It may be blank, but shouldn't be in production. Param db is the id of the
// Redis database.
func NewRedisClient(address, password string, db int) *RedisClient {
	return &RedisClient{
		client: redis.NewClient(&redis.Options{
			Addr:     address,
			Password: password,
			DB:       db,
		}),
	}
}

// Ping pings the Redis server. It should return "PONG" if the server is
// running and we can connect.
func (c *RedisClient) Ping() (string, error) {
	return c.client.Ping().Result()
}

// IngestObjectGet returns an IngestObject from Redis.
func (c *RedisClient) IngestObjectGet(workItemId int, objIdentifier string) (*service.IngestObject, error) {
	key := strconv.Itoa(workItemId)
	field := fmt.Sprintf("object:%s", objIdentifier)
	data, err := c.client.HGet(key, field).Result()
	if err != nil {
		return nil, fmt.Errorf("IngestObjectGet (%d, %s): %s",
			workItemId, objIdentifier, err.Error())
	}
	return service.IngestObjectFromJson(data)
}

// IngestObjectSave saves an IngestObject to Redis.
func (c *RedisClient) IngestObjectSave(workItemId int, obj *service.IngestObject) error {
	key := strconv.Itoa(workItemId)
	field := fmt.Sprintf("object:%s", obj.Identifier())
	jsonData, err := obj.ToJson()
	if err != nil {
		return err
	}
	_, err = c.client.HSet(key, field, jsonData).Result()
	return err
}

// IngestObjectDelete deletes an IngestObject from Redis.
// Note that this deletes the object record only, not the file records.
func (c *RedisClient) IngestObjectDelete(workItemId int, objIdentifier string) error {
	key := strconv.Itoa(workItemId)
	field := fmt.Sprintf("object:%s", objIdentifier)
	_, err := c.client.HDel(key, field).Result()
	return err
}

// IngestFileGet returns an IngestFile from Redis.
func (c *RedisClient) IngestFileGet(workItemId int, fileIdentifier string) (*service.IngestFile, error) {
	key := strconv.Itoa(workItemId)
	field := fmt.Sprintf("file:%s", fileIdentifier)
	data, err := c.client.HGet(key, field).Result()
	if err != nil {
		return nil, fmt.Errorf("IngestFileGet (%d, %s): %s",
			workItemId, fileIdentifier, err.Error())
	}
	return service.IngestFileFromJson(data)
}

// IngestFileSave saves an IngestFile to Redis.
func (c *RedisClient) IngestFileSave(workItemId int, f *service.IngestFile) error {
	key := strconv.Itoa(workItemId)
	field := fmt.Sprintf("file:%s", f.Identifier())
	jsonData, err := f.ToJson()
	if err != nil {
		return err
	}
	_, err = c.client.HSet(key, field, jsonData).Result()
	return err
}

// IngestFileDelete deletes an IngestFile from Redis.
func (c *RedisClient) IngestFileDelete(workItemId int, fileIdentifier string) error {
	key := strconv.Itoa(workItemId)
	field := fmt.Sprintf("file:%s", fileIdentifier)
	_, err := c.client.HDel(key, field).Result()
	return err
}

// WorkItemDelete deletes the Redis copy (NOT the Pharos copy) of a WorkItem,
// along with its associated IngestObject and IngestFile records. Call
// this only when ingest is complete and no further workers will need to
// access the working data.
func (c *RedisClient) WorkItemDelete(workItemId int) (int64, error) {
	key := strconv.Itoa(workItemId)
	return c.client.Del(key).Result()
}
