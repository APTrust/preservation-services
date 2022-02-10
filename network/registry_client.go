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
	relativeURL := fmt.Sprintf("/admin-api/%s/institutions/?%s", client.APIVersion, encodeParams(params))
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

/**********************************************************************

Commented functions below will be needed later for integration and
end-to-end tests. Implement these after the core functionality is
complete.

***********************************************************************/

// // IntellectualObjectRequestRestore creates a restore request in Registry for
// // the object with the specified identifier. This is used in integration
// // testing to create restore requests. Note that this call should issue
// // to requests to Registry. The first creates the restore request, and the
// // second returns the WorkItem for the restore request.
// func (client *RegistryClient) IntellectualObjectRequestRestore(identifier string) *RegistryResponse {
// 	// Set up the response object
// 	resp := NewRegistryResponse(RegistryWorkItem)
// 	resp.workItems = make([]*registry.WorkItem, 1)

// 	// Build the url and the request object
// 	relativeURL := fmt.Sprintf("/admin-api/%s/objects/%s/restore", client.APIVersion, EscapeFileIdentifier(identifier))
// 	absoluteURL := client.BuildURL(relativeURL)

// 	// Run the request.
// 	client.DoRequest(resp, "PUT", absoluteURL, nil)
// 	if resp.Error != nil {
// 		return resp
// 	}

// 	acknowledgment := Acknowledgment{}
// 	resp.Error = json.Unmarshal(resp.data, &acknowledgment)
// 	if resp.Error == nil && acknowledgment.WorkItemID != 0 {
// 		return client.WorkItemByID(acknowledgment.WorkItemID)
// 	}
// 	if acknowledgment.Message != "" {
// 		resp.Error = fmt.Errorf("Registry returned status %s: %s",
// 			acknowledgment.Status, acknowledgment.Message)
// 	}
// 	return resp
// }

// // IntellectualObjectRequestDelete creates a delete request in Registry for
// // the object with the specified identifier. This is used in integration
// // testing to create a set of file deletion requests. This call returns no
// // data.
// func (client *RegistryClient) IntellectualObjectRequestDelete(identifier string) *RegistryResponse {
// 	// Set up the response object, but note that this call returns
// 	// no data.
// 	resp := NewRegistryResponse(RegistryIntellectualObject)
// 	resp.objects = make([]*registry.IntellectualObject, 0)

// 	// Build the url and the request object
// 	relativeURL := fmt.Sprintf("/admin-api/%s/objects/%s/delete", client.APIVersion, EscapeFileIdentifier(identifier))
// 	absoluteURL := client.BuildURL(relativeURL)

// 	// Run the request.
// 	client.DoRequest(resp, "DELETE", absoluteURL, nil)
// 	if resp.Error != nil {
// 		return resp
// 	}
// 	if resp.Response.StatusCode != 200 && resp.Response.StatusCode != 204 {
// 		bytes, _ := resp.RawResponseData()
// 		resp.Error = fmt.Errorf("Registry returned response code %d. Response: %s",
// 			resp.Response.StatusCode, string(bytes))
// 	}
// 	return resp
// }

// // IntellectualObjectFinishDelete tells Registry to mark an IntellectualObject
// // as deleted, once we've finished deleting it.
// func (client *RegistryClient) IntellectualObjectFinishDelete(identifier string) *RegistryResponse {
// 	// Set up the response object
// 	resp := NewRegistryResponse(RegistryIntellectualObject)
// 	resp.objects = make([]*registry.IntellectualObject, 0)

// 	// Build the url and the request object
// 	relativeURL := fmt.Sprintf("/admin-api/%s/objects/%s/finish_delete", client.APIVersion,
// 		EscapeFileIdentifier(identifier))
// 	absoluteURL := client.BuildURL(relativeURL)

// 	// Run the request
// 	client.DoRequest(resp, "GET", absoluteURL, nil)
// 	if resp.Error != nil {
// 		return resp
// 	}

// 	// This call has no response body. We're just looking for 200 or 204.
// 	if resp.Response.StatusCode != 200 && resp.Response.StatusCode != 204 {
// 		resp.Error = fmt.Errorf("IntellectualObject finish_delete failed with message: %s", string(resp.data))
// 	}
// 	return resp
// }

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

	//institutionIdentifier := params.Get("institution_identifier")
	//params.Del("institution_identifier")

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/files/?%s",
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

// GenericFileSaveBatch saves a batch of Generic File records to Registry.
// This performs a POST to create a new records, so all of the GenericFiles
// passed in param objList should have Ids of zero. Each record
// must also have an IntellectualObject ID. The response object will
// be a list containing a new copy of each GenericFile that was saved.
// The new copies have correct ids and timestamps. On the Registry end,
// the batch insert is run as a transaction, so either all inserts
// succeed, or the whole transaction is rolled back and no inserts
// occur.
func (client *RegistryClient) GenericFileSaveBatch(gfList []*registry.GenericFile) *RegistryResponse {
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
				"passed to GenericFileSaveBatch has a non-zero id. This call " +
				"is for creating new GenericFiles only.")
			return resp
		}
	}

	// URL and method
	relativeURL := fmt.Sprintf("/admin-api/%s/files/%d/create_batch",
		client.APIVersion, gfList[0].IntellectualObjectID)
	httpMethod := "POST"
	absoluteURL := client.BuildURL(relativeURL)

	// Transform into a set of objects that serialize in a way Registry
	// will accept.
	//batch := make([]*registry.GenericFileForRegistry, len(gfList))
	//for i, gf := range gfList {
	//	batch[i] = registry.NewGenericFileForRegistry(gf)
	//}

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

// GenericFileRequestRestore creates a restore request in Registry for
// the file with the specified identifier. This is used in integration
// testing to create restore requests. This call generally issues two
// requests: one asking Registry to create a WorkItem, and a second to
// return the WorkItem. Ideally, Registry should redirecto so we don't have
// to make two calls.
// This is logged as a Registry issue in https://trello.com/c/uE1CFNji
func (client *RegistryClient) GenericFileRequestRestore(identifier string) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryWorkItem)
	resp.workItems = make([]*registry.WorkItem, 1)

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/files/restore/%s", client.APIVersion, url.QueryEscape(identifier))
	absoluteURL := client.BuildURL(relativeURL)

	// Run the request.
	client.DoRequest(resp, "PUT", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}

	acknowledgment := Acknowledgment{}
	resp.Error = json.Unmarshal(resp.data, &acknowledgment)
	if resp.Error == nil && acknowledgment.WorkItemID != 0 {
		return client.WorkItemByID(acknowledgment.WorkItemID)
	}
	if acknowledgment.Message != "" {
		resp.Error = fmt.Errorf("Registry returned status %s: %s",
			acknowledgment.Status, acknowledgment.Message)
	}
	return resp
}

// GenericFileFinishDelete tells Registry we've finished deleting a
// generic file. We have to create the deletion PREMIS event
// before calling this. This call returns no data. If response.Error
// is nil, it succeeded.
func (client *RegistryClient) GenericFileFinishDelete(identifier string) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryGenericFile)
	resp.files = make([]*registry.GenericFile, 1)

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/files/finish_delete/%s", client.APIVersion,
		EscapeFileIdentifier(identifier))
	absoluteURL := client.BuildURL(relativeURL)

	// Run the request
	client.DoRequest(resp, "GET", absoluteURL, nil)
	if resp.Error != nil {
		return resp
	}

	// This call has no response body. We're just looking for 200 or 204.
	if resp.Response.StatusCode != 200 && resp.Response.StatusCode != 204 {
		resp.Error = fmt.Errorf("GenericFile finish_delete failed with message: %s", string(resp.data))
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
// * generic_file_identifier - The identifier of the file to which
//   the checksum belongs.
// * algorithm - The checksum algorithm (constants.AldMd5, constants.AlgSha256)
//
// Registry should support order and limit for this call, but it doesn't.
// Order is "datetime desc" by default, and limit cannot be set.
func (client *RegistryClient) ChecksumList(params url.Values) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryChecksum)
	resp.checksums = make([]*registry.Checksum, 0)

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/checksums/?%s", client.APIVersion, encodeParams(params))
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

// ChecksumSave saves a Checksum to Registry. The checksum Id should be
// zero, since we can create but not update Checksums. Param gfIdentifier
// is the identifier of the GenericFile to which the checksum belongs.
// The response object will have a new copy of the Checksum if the
// save was successful.
func (client *RegistryClient) ChecksumSave(obj *registry.Checksum, gfIdentifier string) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryChecksum)
	resp.checksums = make([]*registry.Checksum, 1)

	// URL and method
	relativeURL := fmt.Sprintf("/admin-api/%s/checksums/%s", client.APIVersion,
		url.QueryEscape(gfIdentifier))
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
func (client *RegistryClient) PremisEventByID(id int) *RegistryResponse {
	// Set up the response object
	relativeURL := fmt.Sprintf("/admmin_api/%s/events/show/%d", client.APIVersion, id)
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
// * object_identifier - (string) Return events associated with
//   the specified intellectual object (but not its generic files).
// * file_identifier - (string) Return events associated with the
//   specified generic file. NOTE THAT THIS DIFFERS FROM OTHER CALLS,
//   WHICH use generic_file_identifier. (!$?!#! Registry!)
// * event_type - (string) Return events of the specified type. See the
//   event types listed in contants/constants.go
// * created_after - (iso 8601 datetime string) Return events created
//   on or after the specified datetime.
func (client *RegistryClient) PremisEventList(params url.Values) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryPremisEvent)
	resp.events = make([]*registry.PremisEvent, 0)

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/events/?%s", client.APIVersion, encodeParams(params))
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
	relativeURL := fmt.Sprintf("/admin-api/%s/events/", client.APIVersion)
	httpMethod := "POST"
	if obj.ID > 0 {
		// PUT is not even implemented in Registry, and never will be
		relativeURL = fmt.Sprintf("%s/%s", relativeURL, url.QueryEscape(obj.Identifier))
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
	event := &registry.PremisEvent{}
	resp.Error = json.Unmarshal(resp.data, event)
	if resp.Error == nil {
		resp.events[0] = event
	}
	return resp
}

// StorageRecordList returns a list of StorageRecords.
// Param genericFileIdentifier is required.
func (client *RegistryClient) StorageRecordList(genericFileIdentifier string) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryStorageRecord)
	resp.storageRecords = make([]*registry.StorageRecord, 0)

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/storage_records/%s",
		client.APIVersion,
		EscapeFileIdentifier(genericFileIdentifier))
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

// StorageRecordSave saves a StorageRecord to Registry. Note that
// StorageRecords can be created but not updated.
func (client *RegistryClient) StorageRecordSave(obj *registry.StorageRecord, gfIdentifier string) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryStorageRecord)
	resp.storageRecords = make([]*registry.StorageRecord, 1)

	// URL and method
	relativeURL := fmt.Sprintf("/admin-api/%s/storage_records/%s", client.APIVersion,
		url.QueryEscape(gfIdentifier))
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
	cs := &registry.StorageRecord{}
	resp.Error = json.Unmarshal(resp.data, cs)
	if resp.Error == nil {
		resp.storageRecords[0] = cs
	}
	return resp
}

// StorageRecordDelete deletes the storage record with the specified ID.
func (client *RegistryClient) StorageRecordDelete(id int) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryStorageRecord)
	resp.storageRecords = make([]*registry.StorageRecord, 1)

	// URL and method
	relativeURL := fmt.Sprintf("/admin-api/%s/storage_records/%d", client.APIVersion, id)
	httpMethod := "DELETE"
	absoluteURL := client.BuildURL(relativeURL)

	// Run the request
	client.DoRequest(resp, httpMethod, absoluteURL, nil)
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
// created_before - DateTime in RFC3339 format
// created_after - DateTime in RFC3339 format
// updated_before - DateTime in RFC3339 format
// updated_after - DateTime in RFC3339 format
// bag_date - DateTime in RFC3339 format
// name - Name of the tar file that appeared in the receiving bucket.
// name_contains - Match on partial tar file name
// etag - The etag of the file uploaded to the receiving bucket.
// etag_contains - Match on partial etag.
// object_identifier - The IntellectualObject identifier (null in some WorkItems)
// object_identifier_contains - Match on partial IntelObj
// file_identifier - The GenericFile identifier (null on most WorkItems)
// file_identifier_contains - Match on partiak GenericFile identifier
// status - String enum value from constants. StatusFetch, StatusUnpack, etc.
// stage - String enum value from constants. StageReceive, StageCleanup, etc.
// item_action - String enum value from constants. ActionIngest, ActionRestore, etc.
// access - String enum value from constants.AccessRights.
// state - "A" for active items, "D" for deleted items.
// institution_id - Int: id of institution
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
	relativeURL := fmt.Sprintf("/admin-api/%s/items/", client.APIVersion)
	httpMethod := "POST"
	if obj.ID > 0 {
		// URL should look like /admin-api/v2/items/46956/
		relativeURL = fmt.Sprintf("%s%d/", relativeURL, obj.ID)
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

// FinishRestorationSpotTest tells Registry to send an email to institutional
// admins saying APTrust has randomly restored one of their bags as part of a
// spot test.
func (client *RegistryClient) FinishRestorationSpotTest(workItemID int) *RegistryResponse {
	// Set up the response object
	resp := NewRegistryResponse(RegistryWorkItem)
	resp.workItems = make([]*registry.WorkItem, 1)

	// Build the url and the request object
	relativeURL := fmt.Sprintf("/admin-api/%s/notifications/spot_test_restoration/%d/", client.APIVersion, workItemID)
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

	// Registry is still using old Pharos auth headers.
	// Maybe we should change to Registry headers?
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
