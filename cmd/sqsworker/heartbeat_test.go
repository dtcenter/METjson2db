package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const (
	testQueueURL      = "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
	testReceiptHandle = "test-receipt-handle"
	// Small timeout so heartbeatInterval() returns a fast interval (333ms) for unit tests.
	testVisTimeout = int32(1)
)

type fakeAttributeGetter struct {
	attrs map[string]string
	err   error
}

func (f *fakeAttributeGetter) GetQueueAttributes(_ context.Context, _ *sqs.GetQueueAttributesInput, _ ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &sqs.GetQueueAttributesOutput{Attributes: f.attrs}, nil
}

type fakeVisibilityChanger struct {
	mu    sync.Mutex
	calls []*sqs.ChangeMessageVisibilityInput
	err   error
}

func (f *fakeVisibilityChanger) ChangeMessageVisibility(_ context.Context, params *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, params)
	return &sqs.ChangeMessageVisibilityOutput{}, f.err
}

func (f *fakeVisibilityChanger) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func TestFetchQueueVisibilityTimeout(t *testing.T) {
	tests := []struct {
		name    string
		attrs   map[string]string
		apiErr  error
		want    int32
		wantErr bool
	}{
		{
			name:  "returns parsed timeout",
			attrs: map[string]string{"VisibilityTimeout": "120"},
			want:  120,
		},
		{
			name:    "API error is wrapped and returned",
			apiErr:  errors.New("connection refused"),
			wantErr: true,
		},
		{
			name:    "missing attribute returns error",
			attrs:   map[string]string{},
			wantErr: true,
		},
		{
			name:    "unparseable value returns error",
			attrs:   map[string]string{"VisibilityTimeout": "not-a-number"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeAttributeGetter{attrs: tt.attrs, err: tt.apiErr}
			got, err := fetchQueueVisibilityTimeout(context.Background(), fake, testQueueURL)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

// TestHeartbeatInterval verifies the interval is one third of the visibility timeout.
func TestHeartbeatInterval(t *testing.T) {
	tests := []struct {
		timeoutSecs  int32
		wantInterval time.Duration
	}{
		{30, 10 * time.Second},
		{120, 40 * time.Second},
		{1, time.Second / 3},
	}
	for _, tt := range tests {
		got := heartbeatInterval(tt.timeoutSecs)
		if got != tt.wantInterval {
			t.Errorf("heartbeatInterval(%d) = %v, want %v", tt.timeoutSecs, got, tt.wantInterval)
		}
	}
}

// TestHeartbeat_ExtendsPeriodically verifies that ChangeMessageVisibility is called
// repeatedly with the correct queue URL, receipt handle, and visibility timeout.
func TestHeartbeat_ExtendsPeriodically(t *testing.T) {
	fake := &fakeVisibilityChanger{}

	stop := startVisibilityHeartbeat(context.Background(), fake, testQueueURL, testReceiptHandle, testVisTimeout)
	defer stop()

	// heartbeatInterval(1) == 333ms; sleep 1.5s to guarantee at least 3 ticks.
	time.Sleep(1500 * time.Millisecond)
	stop()

	fake.mu.Lock()
	calls := fake.calls
	fake.mu.Unlock()

	if len(calls) < 3 {
		t.Errorf("expected at least 3 ChangeMessageVisibility calls, got %d", len(calls))
	}
	for i, c := range calls {
		if aws.ToString(c.QueueUrl) != testQueueURL {
			t.Errorf("call %d: wrong QueueUrl %q", i, aws.ToString(c.QueueUrl))
		}
		if aws.ToString(c.ReceiptHandle) != testReceiptHandle {
			t.Errorf("call %d: wrong ReceiptHandle %q", i, aws.ToString(c.ReceiptHandle))
		}
		if c.VisibilityTimeout != testVisTimeout {
			t.Errorf("call %d: expected VisibilityTimeout %d, got %d", i, testVisTimeout, c.VisibilityTimeout)
		}
	}
}

// TestHeartbeat_StopsOnStopFunc verifies that no further calls are made after the
// returned stop func is invoked.
func TestHeartbeat_StopsOnStopFunc(t *testing.T) {
	fake := &fakeVisibilityChanger{}

	stop := startVisibilityHeartbeat(context.Background(), fake, testQueueURL, testReceiptHandle, testVisTimeout)
	time.Sleep(700 * time.Millisecond) // ~2 intervals (333ms each)
	stop()

	countAtStop := fake.callCount()
	if countAtStop == 0 {
		t.Fatal("heartbeat made no calls before stop — ticker may not have fired")
	}

	time.Sleep(700 * time.Millisecond) // wait long enough for additional ticks if goroutine is still running
	countLater := fake.callCount()

	if countLater != countAtStop {
		t.Errorf("heartbeat continued after stop: %d calls at stop, %d calls later", countAtStop, countLater)
	}
}

// TestHeartbeat_StopsOnContextCancel verifies that cancelling the parent context
// stops the heartbeat without requiring the stop func to be called.
func TestHeartbeat_StopsOnContextCancel(t *testing.T) {
	fake := &fakeVisibilityChanger{}

	ctx, cancel := context.WithCancel(context.Background())
	stop := startVisibilityHeartbeat(ctx, fake, testQueueURL, testReceiptHandle, testVisTimeout)
	defer stop()

	time.Sleep(700 * time.Millisecond) // ~2 intervals
	cancel()
	time.Sleep(50 * time.Millisecond) // let the goroutine observe cancellation

	countAtCancel := fake.callCount()
	if countAtCancel == 0 {
		t.Fatal("heartbeat made no calls before context cancel — ticker may not have fired")
	}

	time.Sleep(700 * time.Millisecond)
	countLater := fake.callCount()

	if countLater != countAtCancel {
		t.Errorf("heartbeat continued after context cancel: %d calls at cancel, %d calls later", countAtCancel, countLater)
	}
}
