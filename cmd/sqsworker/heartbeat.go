package main

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// sqsAttributeGetter is the subset of sqs.Client needed to fetch queue attributes.
type sqsAttributeGetter interface {
	GetQueueAttributes(ctx context.Context, params *sqs.GetQueueAttributesInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
}

// fetchQueueVisibilityTimeout retrieves the configured visibility timeout in seconds
func fetchQueueVisibilityTimeout(ctx context.Context, client sqsAttributeGetter, queueURL string) (int32, error) {
	input := &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameVisibilityTimeout,
		},
	}

	result, err := client.GetQueueAttributes(ctx, input)
	if err != nil {
		return 0, fmt.Errorf("failed to get queue attributes: %w", err)
	}

	timeoutStr, ok := result.Attributes[string(types.QueueAttributeNameVisibilityTimeout)]
	if !ok {
		return 0, fmt.Errorf("VisibilityTimeout attribute not found in response")
	}

	timeoutInt, err := strconv.ParseInt(timeoutStr, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse visibility timeout '%s': %w", timeoutStr, err)
	}

	return int32(timeoutInt), nil
}

// heartbeatInterval returns the interval at which the heartbeat should fire: one third
// of the visibility timeout. If one extension fails, two full intervals remain before
// the message becomes visible again.
func heartbeatInterval(visibilityTimeoutSecs int32) time.Duration {
	return time.Duration(visibilityTimeoutSecs) * time.Second / 3
}

// sqsVisibilityChanger is the subset of sqs.Client needed by the visibility heartbeat.
type sqsVisibilityChanger interface {
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

// startVisibilityHeartbeat extends the SQS message visibility timeout every
// (visibilityTimeoutSecs / 3) seconds while processing is ongoing. Without this,
// a message that takes longer than the queue's visibility timeout will become
// visible again and a second worker will start processing the same tarball
// concurrently — corrupting the global state in pkg/state/.
// The returned cancel func must be called when processing completes.
func startVisibilityHeartbeat(ctx context.Context, client sqsVisibilityChanger, queueURL, receiptHandle string, visibilityTimeoutSecs int32) context.CancelFunc {
	interval := heartbeatInterval(visibilityTimeoutSecs)
	hbCtx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				_, err := client.ChangeMessageVisibility(hbCtx, &sqs.ChangeMessageVisibilityInput{
					QueueUrl:          aws.String(queueURL),
					ReceiptHandle:     aws.String(receiptHandle),
					VisibilityTimeout: visibilityTimeoutSecs,
				})
				if err != nil && hbCtx.Err() == nil {
					slog.Warn("failed to extend SQS message visibility", "error", err)
				}
			}
		}
	}()
	return cancel
}
