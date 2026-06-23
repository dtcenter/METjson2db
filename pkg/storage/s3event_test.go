package storage

import (
	"testing"
)

func TestParseS3Event_SingleRecord(t *testing.T) {
	body := `{
		"Records": [{
			"s3": {
				"bucket": {"name": "my-bucket"},
				"object": {"key": "path/to/archive.tar.gz", "size": 123456}
			}
		}]
	}`

	event, err := ParseS3Event(body)
	if err != nil {
		t.Fatalf("ParseS3Event returned error: %v", err)
	}
	if len(event.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(event.Records))
	}
	if event.Records[0].S3.Bucket.Name != "my-bucket" {
		t.Errorf("expected bucket 'my-bucket', got %q", event.Records[0].S3.Bucket.Name)
	}
	if event.Records[0].S3.Object.Key != "path/to/archive.tar.gz" {
		t.Errorf("expected key 'path/to/archive.tar.gz', got %q", event.Records[0].S3.Object.Key)
	}
	if event.Records[0].S3.Object.Size != 123456 {
		t.Errorf("expected size 123456, got %d", event.Records[0].S3.Object.Size)
	}
}

func TestParseS3Event_MultipleRecords(t *testing.T) {
	body := `{
		"Records": [
			{"s3": {"bucket": {"name": "b1"}, "object": {"key": "k1.tar.gz", "size": 100}}},
			{"s3": {"bucket": {"name": "b2"}, "object": {"key": "k2.tar.gz", "size": 200}}}
		]
	}`

	event, err := ParseS3Event(body)
	if err != nil {
		t.Fatalf("ParseS3Event returned error: %v", err)
	}
	if len(event.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(event.Records))
	}
	if event.Records[0].S3.Bucket.Name != "b1" {
		t.Errorf("record[0] bucket: got %q", event.Records[0].S3.Bucket.Name)
	}
	if event.Records[1].S3.Bucket.Name != "b2" {
		t.Errorf("record[1] bucket: got %q", event.Records[1].S3.Bucket.Name)
	}
}

func TestParseS3Event_URLEncodedKey(t *testing.T) {
	body := `{
		"Records": [{
			"s3": {
				"bucket": {"name": "bucket"},
				"object": {"key": "path/with+spaces/file%20name.tar.gz", "size": 50}
			}
		}]
	}`

	event, err := ParseS3Event(body)
	if err != nil {
		t.Fatalf("ParseS3Event returned error: %v", err)
	}
	if event.Records[0].S3.Object.Key != "path/with spaces/file name.tar.gz" {
		t.Errorf("expected decoded key, got %q", event.Records[0].S3.Object.Key)
	}
}

func TestParseS3Event_InvalidJSON(t *testing.T) {
	_, err := ParseS3Event("not valid json")
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestParseS3Event_EmptyRecords(t *testing.T) {
	body := `{"Records": []}`

	event, err := ParseS3Event(body)
	if err != nil {
		t.Fatalf("ParseS3Event returned error: %v", err)
	}
	if len(event.Records) != 0 {
		t.Fatalf("expected 0 records, got %d", len(event.Records))
	}
}

func TestParseS3Event_RealWorldFormat(t *testing.T) {
	body := `{
		"Records": [{
			"eventVersion": "2.1",
			"eventSource": "aws:s3",
			"awsRegion": "us-east-1",
			"eventName": "ObjectCreated:Put",
			"s3": {
				"s3SchemaVersion": "1.0",
				"configurationId": "my-config",
				"bucket": {
					"name": "met-stat-uploads",
					"ownerIdentity": {"principalId": "EXAMPLE"},
					"arn": "arn:aws:s3:::met-stat-uploads"
				},
				"object": {
					"key": "uploads/2024/gfs_stats.tar.gz",
					"size": 5242880,
					"eTag": "abc123",
					"sequencer": "0000"
				}
			}
		}]
	}`

	event, err := ParseS3Event(body)
	if err != nil {
		t.Fatalf("ParseS3Event returned error: %v", err)
	}
	if event.Records[0].S3.Bucket.Name != "met-stat-uploads" {
		t.Errorf("bucket: got %q", event.Records[0].S3.Bucket.Name)
	}
	if event.Records[0].S3.Object.Key != "uploads/2024/gfs_stats.tar.gz" {
		t.Errorf("key: got %q", event.Records[0].S3.Object.Key)
	}
	if event.Records[0].S3.Object.Size != 5242880 {
		t.Errorf("size: got %d", event.Records[0].S3.Object.Size)
	}
}
