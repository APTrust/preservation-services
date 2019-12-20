package network

import (
	"fmt"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/go-redis/redis/v7"
	"strconv"
)

type RedisClient struct {
	client *redis.Client
}

func NewRedisClient(address, password string, db int) *RedisClient {
	return &RedisClient{
		client: redis.NewClient(&redis.Options{
			Addr:     address,
			Password: password,
			DB:       db,
		}),
	}
}

func (c *RedisClient) Ping() (string, error) {
	return c.client.Ping().Result()
}

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

func (c *RedisClient) IngestObjectDelete(workItemId int, objIdentifier string) error {
	key := strconv.Itoa(workItemId)
	field := fmt.Sprintf("object:%s", objIdentifier)
	_, err := c.client.HDel(key, field).Result()
	return err
}

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

func (c *RedisClient) IngestFileDelete(workItemId int, fileIdentifier string) error {
	key := strconv.Itoa(workItemId)
	field := fmt.Sprintf("file:%s", fileIdentifier)
	_, err := c.client.HDel(key, field).Result()
	return err
}
