package bagit

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// BagItProfile represents a DART-type BagItProfile, as described at
// https://aptrust.github.io/dart/BagItProfile.html. This format differs
// slightly from the profiles at
// https://github.com/bagit-profiles/bagit-profiles-specification. The
// DART specification is richer and can describe requirements that the
// other profile format cannot. DART can convert between the two formats
// as described in https://aptrust.github.io/dart-docs/users/bagit/importing/
// and https://aptrust.github.io/dart-docs/users/bagit/exporting/.
type BagItProfile struct {
	AcceptBagItVersion   []string         `json:"acceptBagItVersion"`
	AcceptSerialization  []string         `json:"acceptSerialization"`
	AllowFetchTxt        bool             `json:"allowFetchTxt"`
	BagItProfileInfo     BagItProfileInfo `json:"bagItProfileInfo"`
	Description          string           `json:"description"`
	ManifestsAllowed     []string         `json:"manifestsAllowed"`
	ManifestsRequired    []string         `json:"manifestsRequired"`
	Name                 string           `json:"name"`
	Serialization        string           `json:"serialization"`
	TagFilesAllowed      []string         `json:"tagFilesAllowed"`
	TagManifestsAllowed  []string         `json:"tagManifestsAllowed"`
	TagManifestsRequired []string         `json:"tagManifestsRequired"`
	Tags                 []TagDefinition  `json:"tags"`
}

func BagItProfileLoad(filename string) (*BagItProfile, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	return BagItProfileFromJson(string(data))
}

func BagItProfileFromJson(jsonData string) (*BagItProfile, error) {
	p := &BagItProfile{}
	err := json.Unmarshal([]byte(jsonData), p)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (p *BagItProfile) ToJson() (string, error) {
	bytes, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
