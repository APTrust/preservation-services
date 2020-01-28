package registry

import (
	"encoding/json"
	"time"
)

type Checksum struct {
	Algorithm     string    `json:"algorithm"`
	CreatedAt     time.Time `json:"created_at,omitempty"`
	DateTime      time.Time `json:"datetime"`
	Digest        string    `json:"digest"`
	GenericFileId int       `json:"generic_file_id"`
	Id            int       `json:"id,omitempty"`
	UpdatedAt     time.Time `json:"updated_at,omitempty"`
}

func ChecksumFromJson(jsonData string) (*Checksum, error) {
	c := &Checksum{}
	err := json.Unmarshal([]byte(jsonData), c)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Checksum) ToJson() (string, error) {
	bytes, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
