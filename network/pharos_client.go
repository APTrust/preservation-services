package network

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/APTrust/preservation-services/models/registry"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
)

// PharosClient supports basic calls to the Pharos Admin REST API.
// This client does not support the Member API.
type PharosClient struct {
	hostUrl    string
	apiVersion string
	apiUser    string
	apiKey     string
	httpClient *http.Client
	transport  *http.Transport
}

// NewPharosClient creates a new pharos client. Param hostUrl should
// come from the config.json file.
func NewPharosClient(hostUrl, apiVersion, apiUser, apiKey string) (*PharosClient, error) {
	testsAreRunning := flag.Lookup("test.v") != nil
	if !testsAreRunning && (apiUser == "" || apiKey == "") {
		panic("Env vars PHAROS_API_USER and PHAROS_API_KEY cannot be empty.")
	}
	// see security warning on nil PublicSuffixList here:
	// http://gotour.golang.org/src/pkg/net/http/cookiejar/jar.go?s=1011:1492#L24
	cookieJar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("Can't create cookie jar for HTTP client: %v", err)
	}

	// A.D. 2019-11-18: Disable keep alives because Puma 4 seems to be
	// very aggressive about closing idle connections. This leads to
	// 'connection reset by peer' errors on localhost during integration
	// tests, and to numerous connection reset errors in production.
	transport := &http.Transport{
		//MaxIdleConnsPerHost: 2,
		DisableKeepAlives: true,
	}
	httpClient := &http.Client{Jar: cookieJar, Transport: transport}
	return &PharosClient{
		hostUrl:    hostUrl,
		apiVersion: apiVersion,
		apiUser:    apiUser,
		apiKey:     apiKey,
		httpClient: httpClient,
		transport:  transport}, nil
}

// InstitutionGet returns the institution with the specified identifier.
func (client *PharosClient) InstitutionGet(identifier string) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosInstitution)
	resp.institutions = make([]*registry.Institution, 1)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/institutions/%s/", client.apiVersion, url.QueryEscape(identifier))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
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
func (client *PharosClient) InstitutionList(params url.Values) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosInstitution)
	resp.institutions = make([]*registry.Institution, 0)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/institutions/?%s", client.apiVersion, encodeParams(params))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body.
	// If there's an error, it will be recorded in resp.Error
	resp.UnmarshalJsonList()
	return resp
}

// IntellectualObjectGet returns the object with the specified identifier,
// if it exists. Param identifier is an IntellectualObject identifier
// in the format "institution.edu/object_name".
func (client *PharosClient) IntellectualObjectGet(identifier string) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosIntellectualObject)
	resp.objects = make([]*registry.IntellectualObject, 1)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/objects/%s", client.apiVersion, EscapeFileIdentifier(identifier))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
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
// * institution - Return objects belonging to this institution.
// * updated_since - Return object updated since this date.
// * name_contains - Return objects whose name contains the specified string.
// * name_exact - Return only object with the exact name specified.
// * state = 'A' for active records, 'D' for deleted. Default is 'A'
// * storage_option - "Standard", "Glacier-OH", "Glacier-OR", "Glacier-VA",
// *                  "Glacier-Deep-OH", "Glacier-Deep-OR", "Glacier-Deep-VA"
func (client *PharosClient) IntellectualObjectList(params url.Values) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosIntellectualObject)
	resp.objects = make([]*registry.IntellectualObject, 0)

	institution := params.Get("institution")
	params.Del("institution")

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/objects/%s?%s", client.apiVersion, institution, encodeParams(params))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body.
	// If there's an error, it will be recorded in resp.Error
	resp.UnmarshalJsonList()
	return resp
}

// IntellectualObjectSave saves the intellectual object to Pharos. If the
// object has an ID of zero, this performs a POST to create a new
// Intellectual Object. If the ID is non-zero, this updates the existing
// object with a PUT. The response object will contain a new copy of the
// IntellectualObject if it was successfully saved.
func (client *PharosClient) IntellectualObjectSave(obj *registry.IntellectualObject) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosIntellectualObject)
	resp.objects = make([]*registry.IntellectualObject, 1)

	// URL and method
	// Note that POST URL takes an institution identifier, while
	// the PUT URL takes an object identifier.
	relativeUrl := fmt.Sprintf("/api/%s/objects/%s", client.apiVersion, obj.Institution)
	httpMethod := "POST"
	if obj.Id > 0 {
		// PUT URL looks like /api/v2/objects/college.edu%2Fobject_name
		relativeUrl = fmt.Sprintf("/api/%s/objects/%s", client.apiVersion, EscapeFileIdentifier(obj.Identifier))
		httpMethod = "PUT"
	}
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Prepare the JSON data
	postData, err := obj.SerializeForPharos()
	if err != nil {
		resp.Error = err
	}

	// Run the request
	client.DoRequest(resp, httpMethod, absoluteUrl, bytes.NewBuffer(postData))
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

// IntellectualObjectRequestRestore creates a restore request in Pharos for
// the object with the specified identifier. This is used in integration
// testing to create restore requests. Note that this call should issue
// to requests to Pharos. The first creates the restore request, and the
// second returns the WorkItem for the restore request.
func (client *PharosClient) IntellectualObjectRequestRestore(identifier string) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosWorkItem)
	resp.workItems = make([]*registry.WorkItem, 1)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/objects/%s/restore", client.apiVersion, EscapeFileIdentifier(identifier))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request.
	client.DoRequest(resp, "PUT", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	acknowledgment := Acknowledgment{}
	resp.Error = json.Unmarshal(resp.data, &acknowledgment)
	if resp.Error == nil && acknowledgment.WorkItemId != 0 {
		return client.WorkItemGet(acknowledgment.WorkItemId)
	}
	if acknowledgment.Message != "" {
		resp.Error = fmt.Errorf("Pharos returned status %s: %s",
			acknowledgment.Status, acknowledgment.Message)
	}
	return resp
}

// IntellectualObjectRequestDelete creates a delete request in Pharos for
// the object with the specified identifier. This is used in integration
// testing to create a set of file deletion requests. This call returns no
// data.
func (client *PharosClient) IntellectualObjectRequestDelete(identifier string) *PharosResponse {
	// Set up the response object, but note that this call returns
	// no data.
	resp := NewPharosResponse(PharosIntellectualObject)
	resp.objects = make([]*registry.IntellectualObject, 0)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/objects/%s/delete", client.apiVersion, EscapeFileIdentifier(identifier))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request.
	client.DoRequest(resp, "DELETE", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}
	if resp.Response.StatusCode != 200 && resp.Response.StatusCode != 204 {
		bytes, _ := resp.RawResponseData()
		resp.Error = fmt.Errorf("Pharos returned response code %d. Response: %s",
			resp.Response.StatusCode, string(bytes))
	}
	return resp
}

// IntellectualObjectFinishDelete tells Pharos to mark an IntellectualObject
// as deleted, once we've finished deleting it.
func (client *PharosClient) IntellectualObjectFinishDelete(identifier string) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosIntellectualObject)
	resp.objects = make([]*registry.IntellectualObject, 0)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/objects/%s/finish_delete", client.apiVersion,
		EscapeFileIdentifier(identifier))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// This call has no response body. We're just looking for 200 or 204.
	if resp.Response.StatusCode != 200 && resp.Response.StatusCode != 204 {
		resp.Error = fmt.Errorf("IntellectualObject finish_delete failed with message: %s", string(resp.data))
	}
	return resp
}

// GenericFileGet returns the GenericFile having the specified identifier.
// The identifier should be in the format
// "institution.edu/object_name/path/to/file.ext"
func (client *PharosClient) GenericFileGet(identifier string) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosGenericFile)
	resp.files = make([]*registry.GenericFile, 1)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/files/%s", client.apiVersion, EscapeFileIdentifier(identifier))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
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

// GenericFileList returns a list of Generic Files. Params include:
//
// * institution_identifier - The identifier of the institution to which
//   the files belong.
// * intellectual_object_identifier - The identifier of the object to which
//   the files belong.
// * not_checked_since [datetime] - Returns a list of files that have not
//   had a fixity check since the specified datetime [yyyy-mm-dd]
// * include_relations=true - Include the file's PremisEvents and Checksums
//   in the response.
// * storage_option - "Standard", "Glacier-OH", "Glacier-OR", "Glacier-VA",
//                    "Glacier-Deep-OH", "Glacier-Deep-OR", "Glacier-Deep-VA"
func (client *PharosClient) GenericFileList(params url.Values) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosGenericFile)
	resp.files = make([]*registry.GenericFile, 0)

	institutionIdentifier := params.Get("institution_identifier")
	params.Del("institution_identifier")

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/files/%s?%s",
		client.apiVersion,
		institutionIdentifier,
		encodeParams(params))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body.
	// If there's an error, it will be recorded in resp.Error
	resp.UnmarshalJsonList()
	return resp
}

// GenericFileSave saves a Generic File record to Pharos. If the Generic
// File's ID is zero, this performs a POST to create a new record.
// For non-zero IDs, this performs a PUT to update the existing record.
// Either way, the record must have an IntellectualObject ID. The response
// object will have a new copy of the GenericFile if the save was successful.
func (client *PharosClient) GenericFileSave(obj *registry.GenericFile) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosGenericFile)
	resp.files = make([]*registry.GenericFile, 1)

	// URL and method
	relativeUrl := fmt.Sprintf("/api/%s/files/%s", client.apiVersion, EscapeFileIdentifier(obj.IntellectualObjectIdentifier))
	httpMethod := "POST"
	if obj.Id > 0 {
		// PUT URL looks like /api/v2/files/college.edu%2Fobject_name%2Ffile.xml
		relativeUrl = fmt.Sprintf("/api/%s/files/%s", client.apiVersion, EscapeFileIdentifier(obj.Identifier))
		httpMethod = "PUT"
	}
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Prepare the JSON data
	postData, err := obj.SerializeForPharos()
	if err != nil {
		resp.Error = err
	}

	// Run the request
	client.DoRequest(resp, httpMethod, absoluteUrl, bytes.NewBuffer(postData))
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

// ***********************************************************************
// This may be deprecated in preservation-services. Although it
// has excellent performance benefits, it adds a lot of complexity to the
// failure recovery code in cases where Pharos records some files in a
// batch but not others.
//
// On the other hand, a singl batch calls for 200 files translates to 1200
// individual calls, so it may be worth keeping. We'll know more when we
// can test.
// ***********************************************************************
// GenericFileSaveBatch saves a batch of Generic File records to Pharos.
// This performs a POST to create a new records, so all of the GenericFiles
// passed in param objList should have Ids of zero. Each record
// must also have an IntellectualObject ID. The response object will
// be a list containing a new copy of each GenericFile that was saved.
// The new copies have correct ids and timestamps. On the Pharos end,
// the batch insert is run as a transaction, so either all inserts
// succeed, or the whole transaction is rolled back and no inserts
// occur.
func (client *PharosClient) GenericFileSaveBatch(objList []*registry.GenericFile) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosGenericFile)
	resp.files = make([]*registry.GenericFile, len(objList))

	if len(objList) == 0 {
		resp.Error = fmt.Errorf("GenericFileSaveBatch was asked to save an empty list.")
		return resp
	}
	for _, gf := range objList {
		if gf.Id != 0 {
			resp.Error = fmt.Errorf("One or more GenericFiles in the list " +
				"passed to GenericFileSaveBatch has a non-zero id. This call " +
				"is for creating new GenericFiles only.")
			return resp
		}
	}

	// URL and method
	relativeUrl := fmt.Sprintf("/api/%s/files/%d/create_batch",
		client.apiVersion, objList[0].IntellectualObjectId)
	httpMethod := "POST"
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Transform into a set of objects that serialize in a way Pharos
	// will accept.
	batch := make([]*registry.GenericFileForPharos, len(objList))
	for i, gf := range objList {
		batch[i] = registry.NewGenericFileForPharos(gf)
	}

	// Prepare the JSON data
	postData, err := json.Marshal(batch)
	if err != nil {
		resp.Error = fmt.Errorf("Error marshalling GenericFile batch to JSON: %v", err)
		return resp
	}

	// Run the request
	client.DoRequest(resp, httpMethod, absoluteUrl, bytes.NewBuffer(postData))
	if resp.Error != nil {
		return resp
	}

	resp.UnmarshalJsonList()
	return resp
}

// GenericFileRequestRestore creates a restore request in Pharos for
// the file with the specified identifier. This is used in integration
// testing to create restore requests. This call generally issues two
// requests: one asking Pharos to create a WorkItem, and a second to
// return the WorkItem. Ideally, Pharos should redirecto so we don't have
// to make two calls.
// This is logged as a Pharos issue in https://trello.com/c/uE1CFNji
func (client *PharosClient) GenericFileRequestRestore(identifier string) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosWorkItem)
	resp.workItems = make([]*registry.WorkItem, 1)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/files/restore/%s", client.apiVersion, url.QueryEscape(identifier))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request.
	client.DoRequest(resp, "PUT", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	acknowledgment := Acknowledgment{}
	resp.Error = json.Unmarshal(resp.data, &acknowledgment)
	if resp.Error == nil && acknowledgment.WorkItemId != 0 {
		return client.WorkItemGet(acknowledgment.WorkItemId)
	}
	if acknowledgment.Message != "" {
		resp.Error = fmt.Errorf("Pharos returned status %s: %s",
			acknowledgment.Status, acknowledgment.Message)
	}
	return resp
}

// GenericFileFinishDelete tells Pharos we've finished deleting a
// generic file. We have to create the deletion PREMIS event
// before calling this. This call returns no data. If response.Error
// is nil, it succeeded.
func (client *PharosClient) GenericFileFinishDelete(identifier string) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosGenericFile)
	resp.files = make([]*registry.GenericFile, 1)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/files/finish_delete/%s", client.apiVersion,
		EscapeFileIdentifier(identifier))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// This call has no response body. We're just looking for 200 or 204.
	if resp.Response.StatusCode != 200 && resp.Response.StatusCode != 204 {
		resp.Error = fmt.Errorf("GenericFile finish_delete failed with message: %s", string(resp.data))
	}
	return resp
}

// ---------------------------------------------------------------------
//
// Leaving this commented out for now, since it's not even implemented
// in Pharos. If we do implement it, we can uncomment this.
//
// ---------------------------------------------------------------------
// ChecksumGet returns the checksum with the specified id
// func (client *PharosClient) ChecksumGet(id int) *PharosResponse {
// 	// Set up the response object
// 	resp := NewPharosResponse(PharosChecksum)
// 	resp.checksums = make([]*registry.Checksum, 1)

// 	// Build the url and the request object
// 	relativeUrl := fmt.Sprintf("/api/%s/checksums/%d/", client.apiVersion, id)
// 	absoluteUrl := client.BuildUrl(relativeUrl)

// 	// Run the request
// 	client.DoRequest(resp, "GET", absoluteUrl, nil)
// 	if resp.Error != nil {
// 		return resp
// 	}

// 	// Parse the JSON from the response body
// 	checksum := &registry.Checksum{}
// 	resp.Error = json.Unmarshal(resp.data, checksum)
// 	if resp.Error == nil {
// 		resp.checksums[0] = checksum
// 	}
// 	return resp
// }

// ChecksumList returns a list of checksums. Params include:
//
// * generic_file_identifier - The identifier of the file to which
//   the checksum belongs.
// * algorithm - The checksum algorithm (constants.AldMd5, constants.AlgSha256)
func (client *PharosClient) ChecksumList(params url.Values) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosChecksum)
	resp.checksums = make([]*registry.Checksum, 0)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/checksums/?%s", client.apiVersion, encodeParams(params))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body.
	// If there's an error, it will be recorded in resp.Error
	resp.UnmarshalJsonList()
	return resp
}

// ChecksumSave saves a Checksum to Pharos. The checksum Id should be
// zero, since we can create but not update Checksums. Param gfIdentifier
// is the identifier of the GenericFile to which the checksum belongs.
// The response object will have a new copy of the Checksum if the
// save was successful.
func (client *PharosClient) ChecksumSave(obj *registry.Checksum, gfIdentifier string) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosChecksum)
	resp.checksums = make([]*registry.Checksum, 1)

	// URL and method
	relativeUrl := fmt.Sprintf("/api/%s/checksums/%s", client.apiVersion,
		url.QueryEscape(gfIdentifier))
	httpMethod := "POST"
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Prepare the JSON data
	postData, err := obj.SerializeForPharos()
	if err != nil {
		resp.Error = err
	}

	// Run the request
	client.DoRequest(resp, httpMethod, absoluteUrl, bytes.NewBuffer(postData))
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

// PremisEventGet returns the PREMIS event with the specified identifier.
// The identifier should be a UUID in string format, with dashes. E.g.
// "49a7d6b5-cdc1-4912-812e-885c08e90c68"
func (client *PharosClient) PremisEventGet(identifier string) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosPremisEvent)
	resp.events = make([]*registry.PremisEvent, 1)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/events/%s/", client.apiVersion, url.QueryEscape(identifier))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
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

// PremisEventList returns a list of PREMIS events matching the specified
// criteria. Parameters include:
//
// * object_identifier - (string) Return events associated with
//   the specified intellectual object (but not its generic files).
// * file_identifier - (string) Return events associated with the
//   specified generic file.
// * event_type - (string) Return events of the specified type. See the
//   event types listed in contants/constants.go
// * created_since - (iso 8601 datetime string) Return events created
//   on or after the specified datetime.
func (client *PharosClient) PremisEventList(params url.Values) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosPremisEvent)
	resp.events = make([]*registry.PremisEvent, 0)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/events/?%s", client.apiVersion, encodeParams(params))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body.
	// If there's an error, it will be recorded in resp.Error
	resp.UnmarshalJsonList()
	return resp
}

// PremisEventSave saves a PREMIS event to Pharos. If the event ID is zero,
// this issues a POST request to create a new event record. If the ID is
// non-zero, this issues a PUT to update the existing event. The response
// object will have a new copy of the Premis event if the save was successful.
func (client *PharosClient) PremisEventSave(obj *registry.PremisEvent) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosPremisEvent)
	resp.events = make([]*registry.PremisEvent, 1)

	// URL and method
	relativeUrl := fmt.Sprintf("/api/%s/events/", client.apiVersion)
	httpMethod := "POST"
	if obj.Id > 0 {
		// PUT is not even implemented in Pharos, and never will be
		relativeUrl = fmt.Sprintf("%s/%s", relativeUrl, url.QueryEscape(obj.Identifier))
		httpMethod = "PUT"
	}
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Prepare the JSON data
	postData, err := obj.SerializeForPharos()
	if err != nil {
		resp.Error = err
	}

	// Run the request
	client.DoRequest(resp, httpMethod, absoluteUrl, bytes.NewBuffer(postData))
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

// WorkItemGet returns the WorkItem with the specified ID.
func (client *PharosClient) WorkItemGet(id int) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosWorkItem)
	resp.workItems = make([]*registry.WorkItem, 1)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/items/%d/", client.apiVersion, id)
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
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
func (client *PharosClient) WorkItemList(params url.Values) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosWorkItem)
	resp.workItems = make([]*registry.WorkItem, 0)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/items/?%s", client.apiVersion, encodeParams(params))
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
	if resp.Error != nil {
		return resp
	}

	// Parse the JSON from the response body.
	// If there's an error, it will be recorded in resp.Error
	resp.UnmarshalJsonList()
	return resp
}

// WorkItemSave saves a WorkItem record to Pharos. If the WorkItems's ID
// is zero, this performs a POST to create a new record. For non-zero IDs, this
// performs a PUT to update the existing record. The response object
// will include a new copy of the WorkItem if it was saved successfully.
func (client *PharosClient) WorkItemSave(obj *registry.WorkItem) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosWorkItem)
	resp.workItems = make([]*registry.WorkItem, 1)

	// URL and method
	relativeUrl := fmt.Sprintf("/api/%s/items/", client.apiVersion)
	httpMethod := "POST"
	if obj.Id > 0 {
		// URL should look like /api/v2/items/46956/
		relativeUrl = fmt.Sprintf("%s%d/", relativeUrl, obj.Id)
		httpMethod = "PUT"
	}
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Prepare the JSON data
	postData, err := obj.SerializeForPharos()
	if err != nil {
		resp.Error = err
	}

	// Run the request
	client.DoRequest(resp, httpMethod, absoluteUrl, bytes.NewBuffer(postData))
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

// FinishRestorationSpotTest tells Pharos to send an email to institutional
// admins saying APTrust has randomly restored one of their bags as part of a
// spot test.
func (client *PharosClient) FinishRestorationSpotTest(workItemId int) *PharosResponse {
	// Set up the response object
	resp := NewPharosResponse(PharosWorkItem)
	resp.workItems = make([]*registry.WorkItem, 1)

	// Build the url and the request object
	relativeUrl := fmt.Sprintf("/api/%s/notifications/spot_test_restoration/%d/", client.apiVersion, workItemId)
	absoluteUrl := client.BuildUrl(relativeUrl)

	// Run the request
	client.DoRequest(resp, "GET", absoluteUrl, nil)
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

// BuildUrl combines the host and protocol in client.hostUrl with
// relativeUrl to create an absolute URL. For example, if client.hostUrl
// is "http://localhost:3456", then client.BuildUrl("/path/to/action.json")
// would return "http://localhost:3456/path/to/action.json".
func (client *PharosClient) BuildUrl(relativeUrl string) string {
	return client.hostUrl + relativeUrl
}

// NewJsonRequest returns a new request with headers indicating
// JSON request and response formats.
//
// Param method can be "GET", "POST", or "PUT". The Pharos service
// currently only supports those three.
//
// Param absoluteUrl should be the absolute URL. For get requests,
// include params in the query string rather than in the
// requestData param.
//
// Param requestData will be nil for GET requests, and can be
// constructed from bytes.NewBuffer([]byte) for POST and PUT.
// For the PharosClient, we're typically sending JSON data in
// the request body.
func (client *PharosClient) NewJsonRequest(method, absoluteUrl string, requestData io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, absoluteUrl, requestData)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("X-Pharos-API-User", client.apiUser)
	req.Header.Add("X-Pharos-API-Key", client.apiKey)
	req.Header.Add("Connection", "Keep-Alive")

	// Unfix the URL that golang net/url "fixes" for us.
	// URLs that contain %2F (encoded slashes) MUST preserve
	// the %2F. The Go URL library silently converts those
	// to slashes, and we DON'T want that!
	// See http://stackoverflow.com/questions/20847357/golang-http-client-always-escaped-the-url/
	incorrectUrl, err := url.Parse(absoluteUrl)
	if err != nil {
		return nil, err
	}
	opaqueUrl := strings.Replace(absoluteUrl, client.hostUrl, "", 1)

	// This fixes an issue with GenericFile names that include spaces.
	opaqueUrl = strings.Replace(opaqueUrl, " ", "%20", -1)

	correctUrl := &url.URL{
		Scheme: incorrectUrl.Scheme,
		Host:   incorrectUrl.Host,
		Opaque: opaqueUrl,
	}
	req.URL = correctUrl
	return req, nil
}

// DoRequest issues an HTTP request, reads the response, and closes the
// connection to the remote server.
//
// Param resp should be a PharosResponse.
//
// For a description of the other params, see NewJsonRequest.
//
// If an error occurs, it will be recorded in resp.Error.
func (client *PharosClient) DoRequest(resp *PharosResponse, method, absoluteUrl string, requestData io.Reader) {
	// Build the request
	request, err := client.NewJsonRequest(method, absoluteUrl, requestData)
	resp.Request = request
	resp.Error = err
	if resp.Error != nil {
		return
	}

	// Issue the HTTP request
	resp.Response, resp.Error = client.httpClient.Do(request)
	if resp.Error != nil {
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
		resp.Error = fmt.Errorf("Server returned status code %d. Body: %s",
			resp.Response.StatusCode, string(body))
	}
}

func EscapeFileIdentifier(identifier string) string {
	encoded := url.QueryEscape(identifier)
	return strings.Replace(encoded, "+", "%20", -1)
}

func encodeParams(params url.Values) string {
	if params == nil {
		return ""
	} else {
		return params.Encode()
	}
}

// Acknowledgement is an ad-hoc JSON struct that Pharos returns to
// tell us if it did or did not create a WorkItem for our request.
// TODO: Pharos should return consistent stuct formats,
// so we don't have to handle special cases inline like this.
// This is logged as a Pharos issue in https://trello.com/c/uE1CFNji
type Acknowledgment struct {
	Status     string `json:"status"`
	Message    string `json:"message"`
	WorkItemId int    `json:"work_item_id"`
}
