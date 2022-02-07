//go:build integration
// +build integration

package network_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/network"
	"github.com/stretchr/testify/assert"
)

var registryObjTypes = []network.RegistryObjectType{
	network.RegistryIntellectualObject,
	network.RegistryInstitution,
	network.RegistryGenericFile,
	network.RegistryPremisEvent,
	network.RegistryWorkItem,
}

func TestNewRegistryResponse(t *testing.T) {
	for _, objType := range registryObjTypes {
		resp := network.NewRegistryResponse(objType)
		assert.NotNil(t, resp)
		assert.Equal(t, objType, resp.ObjectType())
		assert.Equal(t, 0, resp.Count)
		assert.Nil(t, resp.Next)
		assert.Nil(t, resp.Previous)
	}
}

func TestRegistryRawResponseData(t *testing.T) {
	client := GetRegistryClient(t)
	resp := client.InstitutionByIdentifier("test.edu")

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

func TestRegistryObjectNotFound(t *testing.T) {
	resp := network.NewRegistryResponse(network.RegistryIntellectualObject)
	resp.Response = &http.Response{
		StatusCode: 200,
	}
	assert.False(t, resp.ObjectNotFound())

	resp.Response.StatusCode = 404
	assert.True(t, resp.ObjectNotFound())
}

func TestRegistryObjectType(t *testing.T) {
	for _, objType := range registryObjTypes {
		resp := network.NewRegistryResponse(objType)
		assert.Equal(t, objType, resp.ObjectType())
	}
}

func TestRegistryHasNextPage(t *testing.T) {
	resp := network.NewRegistryResponse(network.RegistryInstitution)
	assert.False(t, resp.HasNextPage())
	link := "http://example.com"
	resp.Next = &link
	assert.True(t, resp.HasNextPage())
}

func TestRegistryHasPreviousPage(t *testing.T) {
	resp := network.NewRegistryResponse(network.RegistryInstitution)
	assert.False(t, resp.HasPreviousPage())
	link := "http://example.com"
	resp.Previous = &link
	assert.True(t, resp.HasPreviousPage())
}

func TestRegistryParamsForNextPage(t *testing.T) {
	resp := network.NewRegistryResponse(network.RegistryInstitution)
	link := "http://example.com?name=college.edu&page=6&per_page=20"
	resp.Next = &link
	params := resp.ParamsForNextPage()
	assert.Equal(t, 3, len(params))
	assert.Equal(t, "college.edu", params.Get("name"))
	assert.Equal(t, "6", params.Get("page"))
	assert.Equal(t, "20", params.Get("per_page"))
}

func TestRegistryParamsForPreviousPage(t *testing.T) {
	resp := network.NewRegistryResponse(network.RegistryInstitution)
	link := "http://example.com?name=college.edu&page=6&per_page=20"
	resp.Previous = &link
	params := resp.ParamsForPreviousPage()
	assert.Equal(t, 3, len(params))
	assert.Equal(t, "college.edu", params.Get("name"))
	assert.Equal(t, "6", params.Get("page"))
	assert.Equal(t, "20", params.Get("per_page"))
}

func TestRegistryInstitution(t *testing.T) {
	client := GetRegistryClient(t)
	resp := client.InstitutionByIdentifier("test.edu")
	assert.Nil(t, resp.Error)
	assert.NotNil(t, resp.Institution())
}

func TestRegistryInstitutions(t *testing.T) {
	client := GetRegistryClient(t)
	resp := client.InstitutionList(nil)
	assert.Nil(t, resp.Error)
	assert.NotEmpty(t, resp.Institutions())
}

func TestRegistryIntellectualObject(t *testing.T) {
	// Obj identifier is from testdata/registry/intellectual_objects.json
	client := GetRegistryClient(t)
	resp := client.IntellectualObjectByIdentifier("institution2.edu/chocolate")
	d, _ := resp.RawResponseData()
	fmt.Println(string(d))
	assert.Nil(t, resp.Error)
	assert.NotNil(t, resp.IntellectualObject())
}

func TestRegistryIntellectualObjects(t *testing.T) {
	client := GetRegistryClient(t)
	resp := client.IntellectualObjectList(nil)
	assert.Nil(t, resp.Error)
	assert.NotEmpty(t, resp.IntellectualObjects())
}

func TestRegistryGenericFile(t *testing.T) {
	// Obj identifier is from testdata/registry/generic_files.json
	client := GetRegistryClient(t)
	resp := client.GenericFileByIdentifier("institution2.edu/chocolate/picture2")
	assert.Nil(t, resp.Error)
	assert.NotNil(t, resp.GenericFile())
}

func TestRegistryGenericFiles(t *testing.T) {
	client := GetRegistryClient(t)
	resp := client.GenericFileList(nil)
	assert.Nil(t, resp.Error)
	assert.NotEmpty(t, resp.GenericFiles())
}

func testRegistryGetChecksum(t *testing.T, client *network.RegistryClient, checksum *registry.Checksum) {
	resp := client.ChecksumByID(checksum.ID)
	assert.Nil(t, resp.Error)
	assert.NotNil(t, resp.Checksum(), checksum.ID)
}

func TestRegistryChecksums(t *testing.T) {
	client := GetRegistryClient(t)
	resp := client.ChecksumList(nil)
	assert.Nil(t, resp.Error)
	checksums := resp.Checksums()
	assert.NotEmpty(t, checksums)
	for _, checksum := range checksums {
		testRegistryGetChecksum(t, client, checksum)
	}
}

func TestRegistryPremisEvent(t *testing.T) {
	// Event identifier is from testdata/registry/premis_events.json
	client := GetRegistryClient(t)
	resp := client.PremisEventByIdentifier("ac6a2b51-a2f4-4380-a3ca-8fa1d45ed6a6")
	assert.Nil(t, resp.Error)
	assert.NotNil(t, resp.PremisEvent())
}

func TestRegistryPremisEvents(t *testing.T) {
	client := GetRegistryClient(t)
	resp := client.PremisEventList(nil)
	assert.Nil(t, resp.Error)
	assert.NotEmpty(t, resp.PremisEvents())
}

func testRegistryWorkItem(t *testing.T, client *network.RegistryClient, item *registry.WorkItem) {
	resp := client.WorkItemByID(item.ID)
	assert.Nil(t, resp.Error)
	workItem := resp.WorkItem()
	assert.NotNil(t, workItem)
	assert.Equal(t, item.ID, workItem.ID)
}

func TestRegistryWorkItems(t *testing.T) {
	client := GetRegistryClient(t)
	resp := client.WorkItemList(nil)
	fmt.Println(resp.Request)
	assert.Nil(t, resp.Error)
	items := resp.WorkItems()
	assert.NotEmpty(t, items)
	for _, item := range items {
		testRegistryWorkItem(t, client, item)
	}
}
