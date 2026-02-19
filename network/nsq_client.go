package network

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
)

type NSQClient struct {
	URL string
}

// NSQStatsData contains the important info returned by a call
// to NSQ's /stats endpoint, including the number of items in each
// topic and queue.
type NSQStatsData struct {
	Version   string          `json:"version"`
	Health    string          `json:"status_code"`
	StartTime uint64          `json:"start_time"`
	Topics    []NSQTopicStats `json:"topics"`
}

type NSQTopicStats struct {
	TopicName    string            `json:"topic_name"`
	Channels     []NSQChannelStats `json:"channels"`
	Depth        int64             `json:"depth"`
	BackendDepth int64             `json:"backend_depth"`
	MessageCount uint64            `json:"message_count"`
	MessageBytes uint64            `json:"message_bytes"`
	Paused       bool              `json:"paused"`

	E2eProcessingLatency QuantileResult `json:"e2e_processing_latency"`
}

type NSQChannelStats struct {
	ChannelName   string             `json:"channel_name"`
	Depth         int64              `json:"depth"`
	BackendDepth  int64              `json:"backend_depth"`
	InFlightCount int                `json:"in_flight_count"`
	DeferredCount int                `json:"deferred_count"`
	MessageCount  uint64             `json:"message_count"`
	RequeueCount  uint64             `json:"requeue_count"`
	TimeoutCount  uint64             `json:"timeout_count"`
	ClientCount   int                `json:"client_count"`
	Clients       []NSQClientV2Stats `json:"clients"`
	Paused        bool               `json:"paused"`

	E2eProcessingLatency QuantileResult `json:"e2e_processing_latency"`
}

type QuantileResult struct {
	Count       int                  `json:"count"`
	Percentiles []map[string]float64 `json:"percentiles"`
}

type NSQClientV2Stats struct {
	ClientID        string `json:"client_id"`
	Hostname        string `json:"hostname"`
	Version         string `json:"version"`
	RemoteAddress   string `json:"remote_address"`
	State           int32  `json:"state"`
	ReadyCount      int64  `json:"ready_count"`
	InFlightCount   int64  `json:"in_flight_count"`
	MessageCount    uint64 `json:"message_count"`
	FinishCount     uint64 `json:"finish_count"`
	RequeueCount    uint64 `json:"requeue_count"`
	ConnectTime     int64  `json:"connect_ts"`
	SampleRate      int32  `json:"sample_rate"`
	Deflate         bool   `json:"deflate"`
	Snappy          bool   `json:"snappy"`
	UserAgent       string `json:"user_agent"`
	Authed          bool   `json:"authed,omitempty"`
	AuthIdentity    string `json:"auth_identity,omitempty"`
	AuthIdentityURL string `json:"auth_identity_url,omitempty"`

	PubCounts []PubCount `json:"pub_counts,omitempty"`

	TLS                           bool   `json:"tls"`
	CipherSuite                   string `json:"tls_cipher_suite"`
	TLSVersion                    string `json:"tls_version"`
	TLSNegotiatedProtocol         string `json:"tls_negotiated_protocol"`
	TLSNegotiatedProtocolIsMutual bool   `json:"tls_negotiated_protocol_is_mutual"`
}

type PubCount struct {
	Topic string `json:"topic"`
	Count uint64 `json:"count"`
}

type ChannelSummary struct {
	InFlightCount int64
	MessageCount  uint64
	FinishCount   uint64
	RequeueCount  uint64
}

func (data *NSQStatsData) GetTopic(name string) *NSQTopicStats {
	for _, topic := range data.Topics {
		if topic.TopicName == name {
			return &topic
		}
	}
	return nil
}

func (data *NSQStatsData) GetChannelSummary(topicName, channelName string) (*ChannelSummary, error) {
	topic := data.GetTopic(topicName)
	if topic == nil {
		return nil, fmt.Errorf("Can't find topic %s", topicName)
	}
	summary := &ChannelSummary{}
	found := false
	for _, c := range topic.Channels {
		if c.ChannelName == channelName {
			found = true
			for _, client := range c.Clients {
				summary.FinishCount += client.FinishCount
				summary.InFlightCount += client.InFlightCount
				summary.MessageCount += client.MessageCount
				summary.RequeueCount += client.RequeueCount
			}
		}
	}
	if !found {
		return nil, fmt.Errorf("Can't find topic/channel %s/%s", topicName, channelName)
	}
	return summary, nil
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
// Param workItemId is the id of the WorkItem record in Registry we want to queue.
func (client *NSQClient) Enqueue(topic string, workItemID int64) error {
	idAsString := strconv.FormatInt(workItemID, 10)
	return client.EnqueueString(topic, idAsString)
}

// EnqueueString posts string data to the specified NSQ topic
func (client *NSQClient) EnqueueString(topic string, data string) error {
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

// GetStats allows us to get some basic stats from NSQ. The NSQ /stats endpoint
// returns a richer set of stats than what this fuction returns, but we only
// need some basic data for integration tests, so that's all we're parsing.
// The return value is a map whose key is the topic name and whose value is
// an NSQTopicStats object. NSQ is supposed to support topic_name as a query
// param, but this doesn't seem to be working in NSQ 0.3.0, so we're just
// returning stats for all topics right now. Also note that requests to
// /stats/ (with trailing slash) produce a 404.
func (client *NSQClient) GetStats() (*NSQStatsData, error) {
	url := fmt.Sprintf("%s/stats?format=json", client.URL)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("NSQ returned status code %d, body: %s",
			resp.StatusCode, body)
	}
	stats := &NSQStatsData{}
	err = json.Unmarshal(body, stats)
	if err != nil {
		return nil, err
	}
	return stats, nil
}
