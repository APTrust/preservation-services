// +build integration

package network_test

import (
	"github.com/APTrust/preservation-services/network"
	"github.com/stretchr/testify/assert"
	"testing"
)

var objectTypes = []network.PharosObjectType{
	network.PharosIntellectualObject,
	network.PharosInstitution,
	network.PharosGenericFile,
	network.PharosPremisEvent,
	network.PharosWorkItem,
}

func TestNewPharosResponse(t *testing.T) {
	for _, objType := range objectTypes {
		resp := network.NewPharosResponse(objType)
		assert.NotNil(t, resp)
		assert.Equal(t, objType, resp.ObjectType())
		assert.Equal(t, 0, resp.Count)
		assert.Nil(t, resp.Next)
		assert.Nil(t, resp.Previous)
	}
}

func TestRawResponseData(t *testing.T) {
	client := GetPharosClient(t)
	resp := client.InstitutionGet("test.edu")

	// Should be able to call repeatedly without error.
	// Incorrect implementation would try to read from
	// closed network socket.
	for i := 0; i < 3; i++ {
		bytes, err := resp.RawResponseData()
		assert.NotNil(t, bytes)
		assert.NotEmpty(t, bytes)
		assert.Nil(t, err)
	}
}

func TestObjectType(t *testing.T) {
	for _, objType := range objectTypes {
		resp := network.NewPharosResponse(objType)
		assert.Equal(t, objType, resp.ObjectType())
	}
}

func TestHasNextPage(t *testing.T) {
	resp := network.NewPharosResponse(network.PharosInstitution)
	assert.False(t, resp.HasNextPage())
	link := "http://example.com"
	resp.Next = &link
	assert.True(t, resp.HasNextPage())
}

func TestHasPreviousPage(t *testing.T) {
	resp := network.NewPharosResponse(network.PharosInstitution)
	assert.False(t, resp.HasPreviousPage())
	link := "http://example.com"
	resp.Previous = &link
	assert.True(t, resp.HasPreviousPage())
}

func TestParamsForNextPage(t *testing.T) {
	resp := network.NewPharosResponse(network.PharosInstitution)
	link := "http://example.com?name=college.edu&page=6&per_page=20"
	resp.Next = &link
	params := resp.ParamsForNextPage()
	assert.Equal(t, 3, len(params))
	assert.Equal(t, "college.edu", params.Get("name"))
	assert.Equal(t, "6", params.Get("page"))
	assert.Equal(t, "20", params.Get("per_page"))
}

func TestParamsForPreviousPage(t *testing.T) {
	resp := network.NewPharosResponse(network.PharosInstitution)
	link := "http://example.com?name=college.edu&page=6&per_page=20"
	resp.Previous = &link
	params := resp.ParamsForPreviousPage()
	assert.Equal(t, 3, len(params))
	assert.Equal(t, "college.edu", params.Get("name"))
	assert.Equal(t, "6", params.Get("page"))
	assert.Equal(t, "20", params.Get("per_page"))
}

func TestInstitution(t *testing.T) {
	client := GetPharosClient(t)
	resp := client.InstitutionGet("test.edu")
	assert.NotNil(t, resp.Institution())
}

func TestInstitutions(t *testing.T) {
	client := GetPharosClient(t)
	resp := client.InstitutionList(nil)
	assert.NotEmpty(t, resp.Institutions())
}

func TestIntellectualObject(t *testing.T) {
	// Obj identifier is from testdata/pharos/intellectual_objects.json
	client := GetPharosClient(t)
	resp := client.IntellectualObjectGet("institution2.edu/chocolate")
	assert.NotNil(t, resp.IntellectualObject())
}

func TestIntellectualObjects(t *testing.T) {
	client := GetPharosClient(t)
	resp := client.IntellectualObjectList(nil)
	assert.NotEmpty(t, resp.IntellectualObjects())
}

func TestGenericFile(t *testing.T) {
	// Obj identifier is from testdata/pharos/generic_files.json
	client := GetPharosClient(t)
	resp := client.GenericFileGet("institution2.edu/chocolate/picture2")
	assert.NotNil(t, resp.GenericFile())
}

func TestGenericFiles(t *testing.T) {
	client := GetPharosClient(t)
	resp := client.GenericFileList(nil)
	assert.NotEmpty(t, resp.GenericFiles())
}

func TestChecksum(t *testing.T) {
	// We have to get the checksums from the db first,
	// because we don't know their ids.
	checksums := GetChecksums(t)
	client := GetPharosClient(t)

	for _, checksum := range checksums {
		resp := client.ChecksumGet(checksum.Id)
		assert.Nil(t, resp.Error)
		assert.NotNil(t, resp.Checksum())
	}
}

func TestChecksums(t *testing.T) {
	client := GetPharosClient(t)
	resp := client.ChecksumList(nil)
	assert.NotEmpty(t, resp.Checksums())
}

func TestPremisEvent(t *testing.T) {
	// Event identifier is from testdata/pharos/premis_events.json
	client := GetPharosClient(t)
	resp := client.PremisEventGet("ac6a2b51-a2f4-4380-a3ca-8fa1d45ed6a6")
	assert.NotNil(t, resp.PremisEvent())
}

func TestPremisEvents(t *testing.T) {
	client := GetPharosClient(t)
	resp := client.PremisEventList(nil)
	assert.NotEmpty(t, resp.PremisEvents())
}

func TestWorkItem(t *testing.T) {
	// ETag comes from fixture data
	etag := "01010101010101010101"
	item := GetWorkItem(t, etag)

	client := GetPharosClient(t)
	resp := client.WorkItemGet(item.Id)
	assert.NotNil(t, resp.WorkItem())
}

func TestWorkItems(t *testing.T) {
	client := GetPharosClient(t)
	resp := client.WorkItemList(nil)
	assert.NotEmpty(t, resp.WorkItems())
}
