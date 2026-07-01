package storage

import (
	"encoding/json"
	"fmt"
	"net/url"
)

type S3Event struct {
	Records []S3EventRecord `json:"Records"` //nolint:tagliatelle
}

type S3EventRecord struct {
	EventVersion string   `json:"eventVersion"`
	EventName    string   `json:"eventName"`
	S3           S3Entity `json:"s3"`
}

type S3Entity struct {
	Bucket S3Bucket `json:"bucket"`
	Object S3Object `json:"object"`
}

type S3Bucket struct {
	Name string `json:"name"`
}

type S3Object struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
}

// ParseS3Event parses an S3 event notification JSON message body.
func ParseS3Event(body string) (S3Event, error) {
	var event S3Event
	if err := json.Unmarshal([]byte(body), &event); err != nil {
		return event, fmt.Errorf("parsing S3 event: %w", err)
	}
	for i := range event.Records {
		key, err := url.QueryUnescape(event.Records[i].S3.Object.Key)
		if err != nil {
			return event, fmt.Errorf("decoding S3 object key %q: %w", event.Records[i].S3.Object.Key, err)
		}
		event.Records[i].S3.Object.Key = key
	}
	return event, nil
}
