package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// fakeSQSHandler implements sqsHandler for unit tests.
type fakeSQSHandler struct {
	mu                sync.Mutex
	visibilityTimeout int32
	deleteCalls       int
}

func (f *fakeSQSHandler) GetQueueAttributes(_ context.Context, _ *sqs.GetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	vt := f.visibilityTimeout
	if vt == 0 {
		vt = 30
	}
	return &sqs.GetQueueAttributesOutput{
		Attributes: map[string]string{
			string(sqstypes.QueueAttributeNameVisibilityTimeout): strconv.Itoa(int(vt)),
		},
	}, nil
}

func (f *fakeSQSHandler) ChangeMessageVisibility(_ context.Context, _ *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeSQSHandler) DeleteMessage(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeSQSHandler) deleteCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deleteCalls
}

// trackingS3Server returns a test HTTP server and a function that reports the URL paths
// of every request the server received. The server returns a 404 NoSuchKey for all requests.
func trackingS3Server(t *testing.T) (s3Client *s3.Client, requestedPaths func() []string) {
	t.Helper()
	var mu sync.Mutex
	var paths []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code><Message>key does not exist</Message></Error>`))
	}))
	t.Cleanup(srv.Close)

	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		config.WithBaseEndpoint(srv.URL),
	)
	if err != nil {
		t.Fatalf("loading AWS config: %v", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) { o.UsePathStyle = true })
	return client, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), paths...)
	}
}

func s3EventBody(t *testing.T, records []map[string]any) string {
	t.Helper()
	b, err := json.Marshal(map[string]any{"Records": records})
	if err != nil {
		t.Fatalf("marshaling S3 event: %v", err)
	}
	return string(b)
}

func s3Record(eventName, bucket, key string) map[string]any {
	return map[string]any{
		"eventName": eventName,
		"s3": map[string]any{
			"bucket": map[string]string{"name": bucket},
			"object": map[string]any{"key": key, "size": 100},
		},
	}
}

// TestHandleMessage_FiltersNonObjectCreatedEvents verifies that non-ObjectCreated events
// are skipped entirely and the message is still deleted from the queue.
// s3Client is nil because these events must never reach S3 GetObject.
func TestHandleMessage_FiltersNonObjectCreatedEvents(t *testing.T) {
	nonCreationEvents := []string{
		"ObjectRemoved:Delete",
		"ObjectRemoved:DeleteMarkerCreated",
		"ObjectRestore:Post",
		"ObjectRestore:Completed",
		"LifecycleExpiration:Delete",
		"", // missing eventName (old or malformed notification)
	}

	for _, eventName := range nonCreationEvents {
		t.Run(eventName, func(t *testing.T) {
			fake := &fakeSQSHandler{}
			body := s3EventBody(t, []map[string]any{
				s3Record(eventName, "my-bucket", "path/to/file.tar.gz"),
			})

			err := handleMessage(context.Background(), fake, nil, testQueueURL, body, testReceiptHandle)
			if err != nil {
				t.Errorf("expected nil error for event %q, got: %v", eventName, err)
			}
			if fake.deleteCount() != 1 {
				t.Errorf("expected message deleted for event %q, got %d DeleteMessage calls", eventName, fake.deleteCount())
			}
		})
	}
}

// TestHandleMessage_PassesObjectCreatedEventToProcessing verifies that ObjectCreated
// events are not filtered and reach S3. We confirm this by checking that the tracking
// server received a GetObject request — a filtered event never touches S3.
func TestHandleMessage_PassesObjectCreatedEventToProcessing(t *testing.T) {
	objectCreatedEvents := []string{
		"ObjectCreated:Put",
		"ObjectCreated:Post",
		"ObjectCreated:Copy",
		"ObjectCreated:CompleteMultipartUpload",
	}

	for _, eventName := range objectCreatedEvents {
		t.Run(eventName, func(t *testing.T) {
			s3Client, requestedPaths := trackingS3Server(t)
			fake := &fakeSQSHandler{}
			body := s3EventBody(t, []map[string]any{
				s3Record(eventName, "my-bucket", "path/to/file.tar.gz"),
			})

			err := handleMessage(context.Background(), fake, s3Client, testQueueURL, body, testReceiptHandle)
			if err != nil {
				t.Errorf("unexpected error for event %q: %v", eventName, err)
			}
			paths := requestedPaths()
			if len(paths) == 0 {
				t.Errorf("event %q: expected S3 GetObject request, but no requests reached the server — event may have been filtered", eventName)
			}
			if fake.deleteCount() != 1 {
				t.Errorf("event %q: expected 1 DeleteMessage call, got %d", eventName, fake.deleteCount())
			}
		})
	}
}

// TestHandleMessage_MixedEventTypes verifies that in a multi-record message, ObjectRemoved
// records are skipped while ObjectCreated records reach S3. Only the ObjectCreated key
// should appear in the S3 server's request log.
func TestHandleMessage_MixedEventTypes(t *testing.T) {
	s3Client, requestedPaths := trackingS3Server(t)
	fake := &fakeSQSHandler{}
	body := s3EventBody(t, []map[string]any{
		s3Record("ObjectRemoved:Delete", "my-bucket", "old-file.tar.gz"),
		s3Record("ObjectCreated:Put", "my-bucket", "new-file.tar.gz"),
	})

	err := handleMessage(context.Background(), fake, s3Client, testQueueURL, body, testReceiptHandle)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	paths := requestedPaths()
	if len(paths) == 0 {
		t.Error("expected an S3 request for the ObjectCreated record, got none")
	}
	for _, p := range paths {
		if strings.Contains(p, "old-file.tar.gz") {
			t.Errorf("S3 request for ObjectRemoved key %q should not have been made", p)
		}
	}
	if fake.deleteCount() != 1 {
		t.Errorf("expected 1 DeleteMessage call, got %d", fake.deleteCount())
	}
}
