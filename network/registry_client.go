package network

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/util"
	"github.com/op/go-logging"
)

// RegistryClient supports basic calls to the Registry Admin REST API.
// This client does not support the Member API.
type RegistryClient struct {
	HostURL    string
	APIVersion string
	APIUser    string
	APIKey     string
	httpClient *http.Client
	logger     *logging.Logger
	transport  *http.Transport
}

// NewRegistryClient creates a new registry client. Param HostUrl should
// come from the config.json file.
func NewRegistryClient(HostURL, APIVersion, APIUser, APIKey string, logger *logging.Logger) (*RegistryClient, error) {
	if !util.TestsAreRunning() && (APIUser == "" || APIKey == "") {
		panic("Env vars REGISTRY_API_USER and REGISTRY_API_KEY cannot be empty.")
	}
	// see security warning on nil PublicSuffixList here:
	// http://gotour.golang.org/src/pkg/net/http/cookiejar/jar.go?s=1011:1492#L24
	cookieJar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("Can't create cookie jar for HTTP client: %v", err)
	}

	// Try to use KeepAlive now that we are off of Puma.
	transport := &http.Transport{
		//MaxIdleConnsPerHost: 2,
		DisableKeepAlives: false,
		ForceAttemptHTTP2: true,
	} 
	
	// This is a workaround for a bug in the Go net/http transport,
	// which never refreshes DNS lookups, even when TTL says to refresh
	// every minute. See https://github.com/golang/go/issues/23427
	// 
	// There's a proposed fix at https://github.com/golang/go/issues/54429,
	// but it's probably months away. Until then, we use this workaround
	// created by the person who filed the initial bug report.
	go func(tr *http.Transport) {
		for {
			time.Sleep(5 * time.Second)
			tr.CloseIdleConnections()
		}
	}(transport)

	httpClient := &http.Client{Jar: cookieJar, Transport: transport}
	return &RegistryClient{
		HostURL:    HostURL,
		APIVersion: APIVersion,
		APIUser:    APIUser,
		APIKey:     APIKey,
		logger:     logger,
		httpClient: httpClient,
		transport:  transport}, nil
}

// InstitutionByIdentifier returns the institution with the specified identifier.
func (client *RegistryClient) InstitutionByIdentifier(identifier string) *RegistryResponse {
	relativeURL := fmt.Sprintf("/admin-api/%s/institutions/show/%s", client.APIVersion, url.QueryEscape(identifier))
	return client.institutionGet(relativeURL)
}

// InstitutionByID returns the institution with the specified id.
func (client *RegistryClient) InstitutionByID(id int64) *RegistryResponse {
	relativeURL := fmt.Sprintf("/admin-api/%s/institutions/show/%d", client.APIVersion, id)
	return client.institutionGet(relativeURL)
}

func (client *RegistryClient) institutionGet(relativeURL string) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryInstitution)
	resp.institutions = make([]*registry.Institution, 1)

	absoluteURL := client.BuildURL(relativeURL)

	// Run the request
	client.DoRequest(resp, "GET", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	institution := &registry.Institution{}
	resp.Error = json.Unmarshal(resp.data, institution)
	if resp.Error == nil {
		resp.institutions[0] = institution
	}
	return resp
}

// InstitutionList returns a list of APTrust depositor institutions.
func (client *RegistryClient) InstitutionList(params url.Values) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryInstitution)
	resp.institutions = make([]*registry.Institution, 0)

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/institutions?%s", client.APIVersion, encodeParams(params))
	absoluteURL := client.BuildURL(relativeURL)

	// Run the request
	client.DoRequest(resp, "GET", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body.
	// If there's an error, it will be recorded in resp.Error
	resp.UnmarshalJSONList()
	return resp
}

// IntellectualObjectByIdentifier returns the object with the specified identifier,
// if it exists. Param identifier is an IntellectualObject identifier
// in the format "institution.edu/object_name".
func (client *RegistryClient) IntellectualObjectByIdentifier(identifier string) *RegistryResponse {
	relativeURL := fmt.Sprintf("/admin-api/%s/objects/show/%s", client.APIVersion, EscapeFileIdentifier(identifier))
	return client.intellectualObjectGet(relativeURL)
}

// IntellectualObjectByID returns the object with the specified id,
// if it exists.
func (client *RegistryClient) IntellectualObjectByID(id int64) *RegistryResponse {
	relativeURL := fmt.Sprintf("/admin-api/%s/objects/show/%d", client.APIVersion, id)
	return client.intellectualObjectGet(relativeURL)
}

func (client *RegistryClient) intellectualObjectGet(relativeURL string) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryIntellectualObject)
	resp.objects = make([]*registry.IntellectualObject, 1)

	// Build the url and the request object
	absoluteURL := client.BuildURL(relativeURL)

	// Run the request
	client.DoRequest(resp, "GET", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	intelObj := &registry.IntellectualObject{}
	resp.Error = json.Unmarshal(resp.data, intelObj)
	if resp.Error == nil {
		resp.objects[0] = intelObj
	}
	return resp
}

// IntellectualObjectList returns a list of IntellectualObjects matching
// the filter criteria specified in params. Params include:
//
// access
// alt_identifier
// bag_group_identifier
// bag_name
// bagit_profile_identifier
// created_at__lteq
// created_at__gteq
// etag
// file_count__gteq
// file_count__lteq
// identifier
// institution_id
// institution_parent_id
// internal_sender_description
// internal_sender_identifier
// size__gteq
// size__lteq
// source_organization
// state
// storage_option
// updated_at__gteq
// updated_at__lteq
//
func (client *RegistryClient) IntellectualObjectList(params url.Values) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryIntellectualObject)
	resp.objects = make([]*registry.IntellectualObject, 0)

	institution := params.Get("institution")
	params.Del("institution")

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/objects/%s?%s", client.APIVersion, institution, encodeParams(params))
	absoluteURL := client.BuildURL(relativeURL)

	// Run the request
	client.DoRequest(resp, "GET", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body.
	// If there's an error, it will be recorded in resp.Error
	resp.UnmarshalJSONList()
	return resp
}

// IntellectualObjectSave saves the intellectual object to Registry. If the
// object has an ID of zero, this performs a POST to create a new
// Intellectual Object. If the ID is non-zero, this updates the existing
// object with a PUT. The response object will contain a new copy of the
// IntellectualObject if it was successfully saved.
func (client *RegistryClient) IntellectualObjectSave(obj *registry.IntellectualObject) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryIntellectualObject)
	resp.objects = make([]*registry.IntellectualObject, 1)

	// URL and method
	// Note that POST URL takes an institution identifier, while
	// the PUT URL takes an object identifier.
	relativeURL := fmt.Sprintf("/admin-api/%s/objects/create/%d", client.APIVersion, obj.InstitutionID)
	httpMethod := "POST"
	if obj.ID > 0 {
		// PUT URL
		relativeURL = fmt.Sprintf("/admin-api/%s/objects/update/%d", client.APIVersion, obj.ID)
		httpMethod = "PUT"
	}
	absoluteURL := client.BuildURL(relativeURL)

	// Prepare the JSON data
	postData, err := obj.ToJSON()
	if err != nil {
		resp.Error = err
	}

	// Run the request
	client.DoRequest(resp, httpMethod, absoluteURL, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	intelObj := &registry.IntellectualObject{}
	resp.Error = json.Unmarshal(resp.data, intelObj)

	if resp.Error == nil {
		resp.objects[0] = intelObj
	}
	return resp
}

// IntellectualObjectDelete tells Registry to mark an IntellectualObject
// as deleted. There are a number of preconditions for this to succeed:
//
// 1. The registry must contain a valid deletion request for this object.
// 2. The deletion request must be approved by an admin at the institution
//    that owns the object.
// 3. There must be a valid ingest work item for this object.
// 4. There must be a valid deletion work item for this object.
// 5. All files belonging to this object must be deleted (that is, state =
//    "D").
//
// Call this method only after you've deleted all the files that make up
// the object.
func (client *RegistryClient) IntellectualObjectDelete(objId int64) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryIntellectualObject)
	resp.objects = make([]*registry.IntellectualObject, 0)

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/objects/delete/%d", client.APIVersion, objId)
	absoluteURL := client.BuildURL(relativeURL)

	// Run the request
	client.DoRequest(resp, "DELETE", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}

	// This call has no response body. We're just looking for 200 or 204.
	if resp.Response.StatusCode != 200 && resp.Response.StatusCode != 204 {
		resp.Error = fmt.Errorf("IntellectualObject finish_delete failed with message: %s", string(resp.data))
	}
	return resp
}

// IntellectualObjectPrepareForDelete prepares a IntellectualObject for deletion
// by setting up the required ingest PremisEvent, DeletionRequest
// and WorkItem. This returns the deletion WorkItem, which you can get with
// resp.WorkItem().
//
// This call is used for integration testing and is
// available only in the test and integration environments. Calling
// this outside those environments will return an error.
func (client *RegistryClient) IntellectualObjectPrepareForDelete(id int64) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryWorkItem)
	resp.workItems = make([]*registry.WorkItem, 1)

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/prepare_object_delete/%d", client.APIVersion, id)
	absoluteURL := client.BuildURL(relativeURL)

	// Run the request
	client.DoRequest(resp, "POST", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	workItem := &registry.WorkItem{}
	resp.Error = json.Unmarshal(resp.data, workItem)
	if resp.Error == nil {
		resp.workItems[0] = workItem
	}
	return resp
}

// GenericFileByIdentifier returns the GenericFile having the specified
// identifier. The identifier should be in the format
// "institution.edu/object_name/path/to/file.ext"
func (client *RegistryClient) GenericFileByIdentifier(identifier string) *RegistryResponse {
	relativeURL := fmt.Sprintf("/admin-api/%s/files/show/%s", client.APIVersion, EscapeFileIdentifier(identifier))
	return client.genericFileGet(relativeURL)
}

// GenericFileByID returns the GenericFile having the specified id.
func (client *RegistryClient) GenericFileByID(id int64) *RegistryResponse {
	relativeURL := fmt.Sprintf("/admin-api/%s/files/show/%d", client.APIVersion, id)
	return client.genericFileGet(relativeURL)
}

func (client *RegistryClient) genericFileGet(relativeURL string) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryGenericFile)
	resp.files = make([]*registry.GenericFile, 1)

	// Build the url and the request object
	absoluteURL := client.BuildURL(relativeURL)

	// Run the request
	client.DoRequest(resp, "GET", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	gf := &registry.GenericFile{}
	resp.Error = json.Unmarshal(resp.data, gf)
	if resp.Error == nil {
		resp.files[0] = gf
	}
	return resp
}

// GenericFileList returns a list of Generic Files. Filter params include:
//
// identifier
// uuid
// intellectual_object_id
// institution_id
// state
// storage_option
// size__gteq
// size__lteq
// created_at__gteq
// created_at__lteq
// updated_at__gteq
// updated_at__lteq
// last_fixity_check__gteq
// last_fixity_check__lteq
//
// Also supports sort, with fields like size__asc, created_at__desc, etc.
// And, of course, page and per_page.
func (client *RegistryClient) GenericFileList(params url.Values) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryGenericFile)
	resp.files = make([]*registry.GenericFile, 0)

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/files?%s",
		client.APIVersion,
		//institutionIdentifier,
		encodeParams(params))
	absoluteURL := client.BuildURL(relativeURL)

	// Run the request
	client.DoRequest(resp, "GET", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body.
	// If there's an error, it will be recorded in resp.Error
	resp.UnmarshalJSONList()
	return resp
}

// GenericFileSave saves a Generic File record to Registry. If the Generic
// File's ID is zero, this performs a POST to create a new record.
// For non-zero IDs, this performs a PUT to update the existing record.
// Either way, the record must have an IntellectualObject ID. The response
// object will have a new copy of the GenericFile if the save was successful.
func (client *RegistryClient) GenericFileSave(gf *registry.GenericFile) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryGenericFile)
	resp.files = make([]*registry.GenericFile, 1)

	// URL and method
	relativeURL := fmt.Sprintf("/admin-api/%s/files/create/%d", client.APIVersion, gf.InstitutionID)
	httpMethod := "POST"
	if gf.ID > 0 {
		relativeURL = fmt.Sprintf("/admin-api/%s/files/update/%d", client.APIVersion, gf.ID)
		httpMethod = "PUT"
	}
	absoluteURL := client.BuildURL(relativeURL)

	// Prepare the JSON data
	postData, err := gf.ToJSON()
	if err != nil {
		resp.Error = err
	}

	// Run the request
	client.DoRequest(resp, httpMethod, absoluteURL, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	savedFile := &registry.GenericFile{}
	resp.Error = json.Unmarshal(resp.data, savedFile)
	if resp.Error == nil {
		resp.files[0] = savedFile
	}
	return resp
}

// GenericFileCreateBatch creates a batch of Generic Files in the Registry.
// This performs a POST to create a new records, so all of the GenericFiles
// passed in param objList should have Ids of zero. Each record
// must also have an IntellectualObject ID. The response object will
// be a list containing a new copy of each GenericFile that was saved.
// The new copies have correct ids and timestamps. On the Registry end,
// the batch insert is run as a transaction, so either all inserts
// succeed, or the whole transaction is rolled back and no inserts
// occur.
func (client *RegistryClient) GenericFileCreateBatch(gfList []*registry.GenericFile) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryGenericFile)
	resp.files = make([]*registry.GenericFile, len(gfList))

	if len(gfList) == 0 {
		resp.Error = fmt.Errorf("GenericFileSaveBatch was asked to save an empty list.")
		return resp
	}
	for _, gf := range gfList {
		if gf.ID != 0 {
			resp.Error = fmt.Errorf("One or more GenericFiles in the list " +
				"passed to GenericFileSaveBatch has a non-zero id. " +
				"This call is for creating new GenericFiles only.")
			return resp
		}
	}

	// URL and method
	relativeURL := fmt.Sprintf("/admin-api/%s/files/create_batch/%d", client.APIVersion, gfList[0].InstitutionID)
	httpMethod := "POST"
	absoluteURL := client.BuildURL(relativeURL)

	// Prepare the JSON data
	postData, err := json.Marshal(gfList)
	if err != nil {
		resp.Error = fmt.Errorf("Error marshalling GenericFile batch to JSON: %v", err)
		return resp
	}

	// Run the request
	client.DoRequest(resp, httpMethod, absoluteURL, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	resp.UnmarshalJSONList()
	return resp
}

// GenericFileDelete deletes a file, creating the necessary deletion
// Premis Event along the way. Remember to mark the WorkItem done afterwards.
//
// The following preconditions must exist for this to succeed:
//
// 1. The registry must contain a valid deletion request for this file,
//    or for its parent object, if this is part of an object deletion.
// 2. The deletion request must be approved by an admin at the institution
//    that owns the object/file.
// 3. There must be a vaild ingest work item for this file's parent object.
// 4. There must be a valid deletion work item for this file or its
//    parent object.
//
func (client *RegistryClient) GenericFileDelete(id int64) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryGenericFile)
	resp.files = make([]*registry.GenericFile, 1)

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/files/delete/%d", client.APIVersion, id)
	absoluteURL := client.BuildURL(relativeURL)

	// Run the request
	client.DoRequest(resp, "DELETE", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}

	// This call has no response body. We're just looking for 200 or 204.
	if resp.Response.StatusCode != 200 && resp.Response.StatusCode != 204 {
		resp.Error = fmt.Errorf("GenericFile finish_delete failed with message: %s", string(resp.data))
	}
	return resp
}

// GenericFilePrepareForDelete prepares a GenericFile for deletion
// by setting up the required ingest PremisEvent, DeletionRequest
// and WorkItem. This returns the deletion WorkItem.
//
// This call is used for integration testing and is
// available only in the test and integration environments. Calling
// this outside those environments will return an error.
func (client *RegistryClient) GenericFilePrepareForDelete(id int64) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryWorkItem)
	resp.workItems = make([]*registry.WorkItem, 1)

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/prepare_file_delete/%d", client.APIVersion, id)
	absoluteURL := client.BuildURL(relativeURL)

	// Run the request
	client.DoRequest(resp, "POST", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	workItem := &registry.WorkItem{}
	resp.Error = json.Unmarshal(resp.data, workItem)
	if resp.Error == nil {
		resp.workItems[0] = workItem
	}
	return resp
}

// ChecksumByID returns the checksum with the specified id
func (client *RegistryClient) ChecksumByID(id int64) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryChecksum)
	resp.checksums = make([]*registry.Checksum, 1)

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/checksums/show/%d", client.APIVersion, id)
	absoluteURL := client.BuildURL(relativeURL)

	// Run the request
	client.DoRequest(resp, "GET", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	checksum := &registry.Checksum{}
	resp.Error = json.Unmarshal(resp.data, checksum)
	if resp.Error == nil {
		resp.checksums[0] = checksum
	}
	return resp
}

// ChecksumList returns a list of checksums. Params include:
//
// algorithm
// date_time__gteq
// date_time__lteq
// digest
// generic_file_id
// generic_file_identifier
// institution_id
// intellectual_object_id
// state
//
func (client *RegistryClient) ChecksumList(params url.Values) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryChecksum)
	resp.checksums = make([]*registry.Checksum, 0)

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/checksums?%s", client.APIVersion, encodeParams(params))
	absoluteURL := client.BuildURL(relativeURL)

	// Run the request
	client.DoRequest(resp, "GET", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body.
	// If there's an error, it will be recorded in resp.Error
	resp.UnmarshalJSONList()
	return resp
}

// ChecksumCreate creates a new Checksum to Registry. The checksum Id
// should be zero, since we can create but not update checksums.
// The response object will have a new copy of the Checksum if the
// save was successful. We should only call this when ingesting a new
// version of a previously ingested file.
func (client *RegistryClient) ChecksumCreate(obj *registry.Checksum) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryChecksum)
	resp.checksums = make([]*registry.Checksum, 1)

	// URL and method
	relativeURL := fmt.Sprintf("/admin-api/%s/checksums/create/%d", client.APIVersion, obj.InstitutionID)
	httpMethod := "POST"
	absoluteURL := client.BuildURL(relativeURL)

	// Prepare the JSON data
	postData, err := obj.ToJSON()
	if err != nil {
		resp.Error = err
	}

	// Run the request
	client.DoRequest(resp, httpMethod, absoluteURL, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	cs := &registry.Checksum{}
	resp.Error = json.Unmarshal(resp.data, cs)
	if resp.Error == nil {
		resp.checksums[0] = cs
	}
	return resp
}

// PremisEventByIdentifier returns the PREMIS event with the specified identifier.
// The identifier should be a UUID in string format, with dashes. E.g.
// "49a7d6b5-cdc1-4912-812e-885c08e90c68"
func (client *RegistryClient) PremisEventByIdentifier(identifier string) *RegistryResponse {
	relativeURL := fmt.Sprintf("/admin-api/%s/events/show/%s", client.APIVersion, url.QueryEscape(identifier))
	return client.premisEventGet(relativeURL)
}

// PremisEventByID returns the PREMIS event with the specified id.
func (client *RegistryClient) PremisEventByID(id int64) *RegistryResponse {
	// Set up the response object
	relativeURL := fmt.Sprintf("/admin-api/%s/events/show/%d", client.APIVersion, id)
	return client.premisEventGet(relativeURL)
}

func (client *RegistryClient) premisEventGet(relativeURL string) *RegistryResponse {
	resp := NewRegistryResponse(RegistryPremisEvent)
	resp.events = make([]*registry.PremisEvent, 1)
	absoluteURL := client.BuildURL(relativeURL)
	client.DoRequest(resp, "GET", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}
	event := &registry.PremisEvent{}
	resp.Error = json.Unmarshal(resp.data, event)
	if resp.Error == nil {
		resp.events[0] = event
	}
	return resp
}

// PremisEventList returns a list of PREMIS events matching the specified
// criteria. Parameters include:
//
// date_time__gteq
// date_time__lteq
// event_type
// generic_file_id
// generic_file_identifier
// identifier
// institution_id
// intellectual_object_id
// intellectual_object_identifier
// outcome
//
func (client *RegistryClient) PremisEventList(params url.Values) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryPremisEvent)
	resp.events = make([]*registry.PremisEvent, 0)

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/events?%s", client.APIVersion, encodeParams(params))
	absoluteURL := client.BuildURL(relativeURL)

	// Run the request
	client.DoRequest(resp, "GET", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body.
	// If there's an error, it will be recorded in resp.Error
	resp.UnmarshalJSONList()
	return resp
}

// PremisEventSave saves a PREMIS event to Registry. If the event ID is zero,
// this issues a POST request to create a new event record. If the ID is
// non-zero, this issues a PUT to update the existing event. The response
// object will have a new copy of the Premis event if the save was successful.
func (client *RegistryClient) PremisEventSave(obj *registry.PremisEvent) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryPremisEvent)
	resp.events = make([]*registry.PremisEvent, 1)

	// URL and method
	relativeURL := fmt.Sprintf("/admin-api/%s/events/create", client.APIVersion)
	httpMethod := "POST"
	if obj.ID > 0 {
		// PUT/update for PremisEvent is not even implemented in Registry,
		// and never will be
		resp.Error = http.ErrNotSupported
		return resp
	}
	absoluteURL := client.BuildURL(relativeURL)

	// Prepare the JSON data
	postData, err := obj.ToJSON()
	if err != nil {
		resp.Error = err
	}

	// Run the request
	client.DoRequest(resp, httpMethod, absoluteURL, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	event := &registry.PremisEvent{}
	resp.Error = json.Unmarshal(resp.data, event)
	if resp.Error == nil {
		resp.events[0] = event
	}
	return resp
}

// StorageRecordCreate creates a new StorageRecord in the Registry. The record Id
// should be zero, since we can create but not update storage records.
// The response object will have a new copy of the StorageRecord if the
// save was successful. This call is used in integration tests, but not in
// production.
func (client *RegistryClient) StorageRecordCreate(obj *registry.StorageRecord, institutionID int64) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryStorageRecord)
	resp.storageRecords = make([]*registry.StorageRecord, 1)

	// URL and method
	relativeURL := fmt.Sprintf("/admin-api/%s/storage_records/create/%d", client.APIVersion, institutionID)
	httpMethod := "POST"
	absoluteURL := client.BuildURL(relativeURL)

	// Prepare the JSON data
	postData, err := obj.ToJSON()
	if err != nil {
		resp.Error = err
	}

	// Run the request
	client.DoRequest(resp, httpMethod, absoluteURL, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	sr := &registry.StorageRecord{}
	resp.Error = json.Unmarshal(resp.data, sr)
	if resp.Error == nil {
		resp.storageRecords[0] = sr
	}
	return resp
}

// StorageRecordList returns a list of StorageRecords. The only supported
// filter param is generic_file_id. This also supports the usual
// page, per_page, and sort params. The main use case for this is to get
// all storage records for a single generic file.
func (client *RegistryClient) StorageRecordList(params url.Values) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryStorageRecord)
	resp.storageRecords = make([]*registry.StorageRecord, 0)

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/storage_records?%s", client.APIVersion, encodeParams(params))
	absoluteURL := client.BuildURL(relativeURL)

	// Run the request
	client.DoRequest(resp, "GET", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body.
	// If there's an error, it will be recorded in resp.Error
	resp.UnmarshalJSONList()
	return resp
}

// WorkItemByID returns the WorkItem with the specified ID.
func (client *RegistryClient) WorkItemByID(id int64) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryWorkItem)
	resp.workItems = make([]*registry.WorkItem, 1)

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/items/show/%d", client.APIVersion, id)
	absoluteURL := client.BuildURL(relativeURL)

	// Run the request
	client.DoRequest(resp, "GET", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	workItem := &registry.WorkItem{}
	resp.Error = json.Unmarshal(resp.data, workItem)
	if resp.Error == nil {
		resp.workItems[0] = workItem
	}
	return resp
}

// WorkItemList lists the work items meeting the specified filters, or
// all work items if no filter params are set. Params include:
//
// action
// alt_identifier
// bag_date__gteq
// bag_date__lteq
// bag_group_identifier
// bagit_profile_identifier
// bucket
// date_processed__gteq
// date_processed__lteq
// etag
// generic_file_identifier
// institution_id
// name
// needs_admin_review
// node__not_null
// object_identifier
// size__gteq
// size__lteq
// stage
// status
// storage_option
// user - user's email address
//
func (client *RegistryClient) WorkItemList(params url.Values) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryWorkItem)
	resp.workItems = make([]*registry.WorkItem, 0)

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/items?%s", client.APIVersion, encodeParams(params))
	absoluteURL := client.BuildURL(relativeURL)

	// Run the request
	client.DoRequest(resp, "GET", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body.
	// If there's an error, it will be recorded in resp.Error
	resp.UnmarshalJSONList()
	return resp
}

// WorkItemSave saves a WorkItem record to Registry. If the WorkItems's ID
// is zero, this performs a POST to create a new record. For non-zero IDs, this
// performs a PUT to update the existing record. The response object
// will include a new copy of the WorkItem if it was saved successfully.
func (client *RegistryClient) WorkItemSave(obj *registry.WorkItem) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryWorkItem)
	resp.workItems = make([]*registry.WorkItem, 1)

	// URL and method
	relativeURL := fmt.Sprintf("/admin-api/%s/items/create/%d", client.APIVersion, obj.InstitutionID)
	httpMethod := "POST"
	if obj.ID > 0 {
		relativeURL = fmt.Sprintf("/admin-api/%s/items/update/%d", client.APIVersion, obj.ID)
		httpMethod = "PUT"
	}
	absoluteURL := client.BuildURL(relativeURL)

	// Prepare the JSON data
	postData, err := obj.ToJSON()
	if err != nil {
		resp.Error = err
	}

	// Run the request
	client.DoRequest(resp, httpMethod, absoluteURL, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body
	workItem := &registry.WorkItem{}
	resp.Error = json.Unmarshal(resp.data, workItem)
	if resp.Error == nil {
		resp.workItems[0] = workItem
	}
	return resp
}

// -------------------------------------------------------------------------
// Utility Methods
// -------------------------------------------------------------------------

// BuildURL combines the host and protocol in client.HostUrl with
// relativeURL to create an absolute URL. For example, if client.HostUrl
// is "http://localhost:3456", then client.BuildURL("/path/to/action.json")
// would return "http://localhost:3456/path/to/action.json".
func (client *RegistryClient) BuildURL(relativeURL string) string {
	return client.HostURL + relativeURL
}

// NewJSONRequest returns a new request with headers indicating
// JSON request and response formats.
//
// Param method can be "GET", "POST", or "PUT". The Registry service
// currently only supports those three.
//
// Param absoluteURL should be the absolute URL. For get requests,
// include params in the query string rather than in the
// requestData param.
//
// Param requestData will be nil for GET requests, and can be
// constructed from bytes.NewBuffer([]byte) for POST and PUT.
// For the RegistryClient, we're typically sending JSON data in
// the request body.
func (client *RegistryClient) NewJSONRequest(method, absoluteURL string, requestData io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, absoluteURL, requestData)
	if err != nil {
		return nil, err
	}

	// TODO: Review Registry auth headers
	// Registry is still using old Pharos auth headers.
	// Maybe we should change to Registry headers?
	// But keep in mind that depositors are using these auth headers as well.
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("X-Pharos-API-User", client.APIUser)
	req.Header.Add("X-Pharos-API-Key", client.APIKey)
	req.Header.Add("Connection", "Keep-Alive")

	// Unfix the URL that golang net/url "fixes" for us.
	// URLs that contain %2F (encoded slashes) MUST preserve
	// the %2F. The Go URL library silently converts those
	// to slashes, and we DON'T want that!
	// See http://stackoverflow.com/questions/20847357/golang-http-client-always-escaped-the-url/
	incorrectURL, err := url.Parse(absoluteURL)
	if err != nil {
		return nil, err
	}
	opaqueURL := strings.Replace(absoluteURL, client.HostURL, "", 1)

	// This fixes an issue with GenericFile names that include spaces.
	opaqueURL = strings.Replace(opaqueURL, " ", "%20", -1)

	correctURL := &url.URL{
		Scheme: incorrectURL.Scheme,
		Host:   incorrectURL.Host,
		Opaque: opaqueURL,
	}
	req.URL = correctURL
	return req, nil
}

// DoRequest issues an HTTP request, reads the response, and closes the
// connection to the remote server.
//
// Param resp should be a RegistryResponse.
//
// For a description of the other params, see NewJsonRequest.
//
// If an error occurs, it will be recorded in resp.Error.
func (client *RegistryClient) DoRequest(resp *RegistryResponse, method, absoluteURL string, requestData io.Reader) {
	// Build the request
	request, err := client.NewJSONRequest(method, absoluteURL, requestData)
	resp.Request = request
	if err != nil {
		resp.Error = fmt.Errorf("%s %s: %s", method, absoluteURL, err.Error())
		return
	}

	// Issue the HTTP request
	reqTime := time.Now()
	resp.Response, resp.Error = client.httpClient.Do(request)
	client.logger.Infof("%s %s completed in %s", method, absoluteURL, time.Since(reqTime))
	if resp.Error != nil {
		resp.Error = fmt.Errorf("%s %s: %s", method, absoluteURL, resp.Error.Error())
		return
	}

	// Read the response data and close the response body.
	// That's the only way to close the remote HTTP connection,
	// which will otherwise stay open indefinitely, causing
	// the system to eventually have too many open files.
	// If there's an error reading the response body, it will
	// be recorded in resp.Error.
	resp.readResponse()

	if resp.Error == nil && resp.Response.StatusCode >= 400 {
		body, _ := resp.RawResponseData()
		resp.Error = fmt.Errorf("Server returned status code %d. "+
			"%s %s - Body: %s",
			resp.Response.StatusCode, method, absoluteURL, string(body))
	}
}

func EscapeFileIdentifier(identifier string) string {
	encoded := url.QueryEscape(identifier)
	return strings.Replace(encoded, "+", "%20", -1)
}

func encodeParams(params url.Values) string {
	if params == nil {
		return ""
	}
	return params.Encode()
}

// Acknowledgement is an ad-hoc JSON struct that Registry returns to
// tell us if it did or did not create a WorkItem for our request.
// TODO: Registry should return consistent stuct formats,
// so we don't have to handle special cases inline like this.
// This is logged as a Registry issue in https://trello.com/c/uE1CFNji
type Acknowledgment struct {
	Status     string `json:"status"`
	Message    string `json:"message"`
	WorkItemID int64  `json:"work_item_id"`
}
