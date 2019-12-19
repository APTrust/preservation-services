package network

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
)

type NSQClient struct {
	URL string
}

// Formally define this so we can generate mocks for testing.
type NSQClientInterface interface {
	Enqueue(topic string, workItemId int) error
}

// NewNSQClient returns a new NSQ client that will connect to the NSQ
// server and the specified url. The URL is typically available through
// Config.NsqdHttpAddress, and usually ends with :4151. This is
// the URL to which we post items we want to queue, and from
// which our workers read.
//
// Note that this client provides write access to queue, so we can
// add things. It does not provide read access. The workers do the
// reading.
func NewNSQClient(url string) *NSQClient {
	return &NSQClient{URL: url}
}

// Enqueue posts data to NSQ, which essentially means putting it into a work
// topic. Param topic is the topic under which you want to queue something.
// For example, prepare_topic, fixity_topic, etc.
// Param workItemId is the id of the WorkItem record in Pharos we want to queue.
func (client *NSQClient) Enqueue(topic string, workItemId int) error {
	idAsString := strconv.Itoa(workItemId)
	return client.enqueueString(topic, idAsString)
}

// EnqueueString posts string data to the specified NSQ topic
func (client *NSQClient) enqueueString(topic string, data string) error {
	url := fmt.Sprintf("%s/pub?topic=%s", client.URL, topic)
	resp, err := http.Post(url, "text/html", bytes.NewBuffer([]byte(data)))
	if err != nil {
		return fmt.Errorf("Nsqd returned an error when queuing data: %v", err)
	}
	if resp == nil {
		return fmt.Errorf("No response from nsqd at '%s'. Is it running?", url)
	}

	// nsqd sends a simple OK. We have to read the response body,
	// or the connection will hang open forever.
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != 200 {
		bodyText := "[no response body]"
		if len(body) > 0 {
			bodyText = string(body)
		}
		return fmt.Errorf("nsqd returned status code %d when attempting to queue data. "+
			"Response body: %s", resp.StatusCode, bodyText)
	}
	return nil
}
