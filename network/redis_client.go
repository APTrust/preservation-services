package network

import (
	"fmt"
	"strconv"
	"time"

	"github.com/APTrust/preservation-services/models/service"
	"github.com/go-redis/redis/v7"
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
func (c *RedisClient) IngestObjectGet(workItemID int64, objIdentifier string) (*service.IngestObject, error) {
	key := strconv.FormatInt(workItemID, 10)
	field := fmt.Sprintf("object:%s", objIdentifier)
	data, err := c.client.HGet(key, field).Result()
	if err != nil {
		return nil, fmt.Errorf("IngestObjectGet (%d, %s): %s",
			workItemID, objIdentifier, err.Error())
	}
	return service.IngestObjectFromJSON(data)
}

// IngestObjectSave saves an IngestObject to Redis.
func (c *RedisClient) IngestObjectSave(workItemID int64, obj *service.IngestObject) error {
	key := strconv.FormatInt(workItemID, 10)
	field := fmt.Sprintf("object:%s", obj.Identifier())
	jsonData, err := obj.ToJSON()
	if err != nil {
		return err
	}
	_, err = c.client.HSet(key, field, jsonData).Result()
	return err
}

// IngestObjectDelete deletes an IngestObject from Redis.
// Note that this deletes the object record only, not the file records.
func (c *RedisClient) IngestObjectDelete(workItemID int64, objIdentifier string) error {
	key := strconv.FormatInt(workItemID, 10)
	field := fmt.Sprintf("object:%s", objIdentifier)
	_, err := c.client.HDel(key, field).Result()
	return err
}

// RestorationObjectGet returns an RestorationObject from Redis.
func (c *RedisClient) RestorationObjectGet(workItemID int64, objIdentifier string) (*service.RestorationObject, error) {
	key := strconv.FormatInt(workItemID, 10)
	field := fmt.Sprintf("restoration:%s", objIdentifier)
	data, err := c.client.HGet(key, field).Result()
	if err != nil {
		return nil, fmt.Errorf("RestorationObjectGet (%d, %s): %s",
			workItemID, objIdentifier, err.Error())
	}
	return service.RestorationObjectFromJSON(data)
}

// RestorationObjectSave saves an RestorationObject to Redis.
func (c *RedisClient) RestorationObjectSave(workItemID int64, obj *service.RestorationObject) error {
	key := strconv.FormatInt(workItemID, 10)
	field := fmt.Sprintf("restoration:%s", obj.Identifier)
	jsonData, err := obj.ToJSON()
	if err != nil {
		return err
	}
	_, err = c.client.HSet(key, field, jsonData).Result()
	return err
}

// RestorationObjectDelete deletes an RestorationObject from Redis.
func (c *RedisClient) RestorationObjectDelete(workItemID int64, objIdentifier string) error {
	key := strconv.FormatInt(workItemID, 10)
	field := fmt.Sprintf("restoration:%s", objIdentifier)
	_, err := c.client.HDel(key, field).Result()
	return err
}

// IngestFileGet returns an IngestFile from Redis.
func (c *RedisClient) IngestFileGet(workItemID int64, fileIdentifier string) (*service.IngestFile, error) {
	key := strconv.FormatInt(workItemID, 10)
	field := fmt.Sprintf("file:%s", fileIdentifier)
	data, err := c.client.HGet(key, field).Result()
	if err != nil {
		return nil, fmt.Errorf("IngestFileGet (%d, %s): %s",
			workItemID, fileIdentifier, err.Error())
	}
	return service.IngestFileFromJSON(data)
}

// IngestFileSave saves an IngestFile to Redis.
func (c *RedisClient) IngestFileSave(workItemID int64, f *service.IngestFile) error {
	key := strconv.FormatInt(workItemID, 10)
	field := fmt.Sprintf("file:%s", f.Identifier())
	jsonData, err := f.ToJSON()
	if err != nil {
		return err
	}
	_, err = c.client.HSet(key, field, jsonData).Result()
	return err
}

// IngestFileDelete deletes an IngestFile from Redis.
func (c *RedisClient) IngestFileDelete(workItemID int64, fileIdentifier string) error {
	key := strconv.FormatInt(workItemID, 10)
	field := fmt.Sprintf("file:%s", fileIdentifier)
	_, err := c.client.HDel(key, field).Result()
	return err
}

// WorkItemDelete deletes the Redis copy (NOT the Registry copy) of a WorkItem,
// along with its associated IngestObject and IngestFile records. Call
// this only when ingest is complete and no further workers will need to
// access the working data.
func (c *RedisClient) WorkItemDelete(workItemID int64) (int64, error) {
	key := strconv.FormatInt(workItemID, 10)
	return c.client.Del(key).Result()
}

// GetBatchOfFileKeys returns a batch of file keys from redis,
// starting at offset and return up to limit results. The string
// slice returned is a list of keys. The int64 value is the offset
// for the next batch. If the int64 is zero, there are no more keys
// to get. See redis_client_test.go for sample usage.
//
// SCAN can return more or less than the number of items requested.
// See https://redis.io/commands/scan
func (c *RedisClient) GetBatchOfFileKeys(workItemID int64, offset uint64, limit int64) (map[string]*service.IngestFile, uint64, error) {
	key := strconv.FormatInt(workItemID, 10)
	keys, nextOffset, err := c.client.HScan(
		key,
		offset,
		"file:*",
		limit).Result()
	if err != nil {
		return nil, uint64(0), fmt.Errorf(
			"Error scanning Redis hash keys for WorkItem %d: %v",
			workItemID, err)
	}
	keysAndValues := make(map[string]*service.IngestFile, len(keys)/2)
	for i, key := range keys {
		if i%2 == 1 {
			continue // this is a value, not a key
		}
		jsonData := keys[i+1]
		ingestFile, err := service.IngestFileFromJSON(jsonData)
		if err != nil {
			return nil, 0, err
		}
		keysAndValues[key] = ingestFile
	}
	return keysAndValues, nextOffset, nil
}

// IngestFilesApply applies function fn to all IngestFiles belonging
// the the specified workItemID. Note that this saves changes applied
// by fn back to Redis.
//
// This stops processing on the first error and returns the number of
// items on which the function was run successfully.
//
// TODO: Change to use IngestFileForeachOptions
func (c *RedisClient) IngestFilesApply(fn service.IngestFileApplyFn, options service.IngestFileApplyOptions) (count int, errors []*service.ProcessingError) {
	var err error
	nextOffset := uint64(0)
	var fileMap map[string]*service.IngestFile
	for {
		// Get a batch of files from Redis
		fileMap, nextOffset, err = c.GetBatchOfFileKeys(
			options.WorkItemID, nextOffset, int64(200))
		if err != nil {
			procErr := service.NewProcessingError(
				options.WorkItemID,
				"",
				err.Error(),
				false,
			)
			errors = append(errors, procErr)
			if len(errors) >= options.MaxErrors {
				return count, errors
			}
		}
		// For each file in the batch...
		for _, ingestFile := range fileMap {
			var procErrors []*service.ProcessingError
			// Apply the function up to Retries times, with the
			// specified interval between retries.
			for attempt := 0; attempt < options.MaxRetries; attempt++ {
				procErrors = fn(ingestFile)
				if len(procErrors) == 0 {
					break
				}
				time.Sleep(time.Duration(options.RetryMs) * time.Millisecond)
			}
			// Keep the processing error only after the last attempt.
			if len(procErrors) > 0 {
				errors = append(errors, procErrors...)
				if len(errors) >= options.MaxErrors {
					return count, errors
				}
			}
			// Save the file back to Redis if options say so.
			if options.SaveChanges {
				err = c.IngestFileSave(options.WorkItemID, ingestFile)
				if err != nil {
					procErr := service.NewProcessingError(
						options.WorkItemID,
						ingestFile.Identifier(),
						err.Error(),
						false,
					)
					errors = append(errors, procErr)
					if len(errors) >= options.MaxErrors {
						return count, errors
					}
				}
			}
			count++
		}
		// If next offset is zero, we've reached the end
		if nextOffset == 0 {
			break
		}
	}
	return count, errors
}

func (c *RedisClient) WorkResultGet(workItemID int64, operationName string) (*service.WorkResult, error) {
	key := strconv.FormatInt(workItemID, 10)
	field := fmt.Sprintf("workresult:%s", operationName)
	data, err := c.client.HGet(key, field).Result()
	if err != nil {
		return nil, fmt.Errorf("WorkResultGet (%d, %s): %s",
			workItemID, operationName, err.Error())
	}
	return service.WorkResultFromJSON(data)
}

func (c *RedisClient) WorkResultSave(workItemID int64, result *service.WorkResult) error {
	key := strconv.FormatInt(workItemID, 10)
	field := fmt.Sprintf("workresult:%s", result.Operation)
	jsonData, err := result.ToJSON()
	if err != nil {
		return err
	}
	_, err = c.client.HSet(key, field, jsonData).Result()
	return err
}

func (c *RedisClient) WorkResultDelete(workItemID int64, operationName string) error {
	key := strconv.FormatInt(workItemID, 10)
	field := fmt.Sprintf("workresult:%s", operationName)
	_, err := c.client.HDel(key, field).Result()
	return err
}

// Keys returns all keys in the Redis DB matching the specified pattern.
// Each key is a WorkItem.ID in string form. It's generally safe to call
// this with pattern "*" because we rarely have more than a few dozen items
// in Redis at any given time.
func (c *RedisClient) Keys(pattern string) ([]string, error) {
	return c.client.Keys(pattern).Result()
}
