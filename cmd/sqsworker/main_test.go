//go:build integration

package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/dtcenter/METjson2db/pkg/core"
	"github.com/dtcenter/METjson2db/pkg/state"
)

const (
	ministackEndpoint = "http://localhost:4566"
	ministackHealth   = ministackEndpoint + "/_ministack/health"
	sqsTestBucket     = "sqsworker-integration-test"
	sqsTestKey        = "uploads/test_stat_files.tar.gz"
	sqsTestDataDir    = "../../test_data"
)

func requireMiniStack(t *testing.T) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(ministackHealth)
	if err != nil {
		t.Fatalf(
			"MiniStack is not reachable at %s: %v\n\n"+
				"This test requires a running MiniStack instance.\n"+
				"Install & Start:  docker run --name ministack -d -p 4566:4566 ministackorg/ministack\n"+
				"Stop:             docker rm -f ministack",
			ministackEndpoint, err,
		)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("MiniStack health check returned status %d", resp.StatusCode)
	}
}

func newTestAWSConfig(t *testing.T) aws.Config {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		config.WithBaseEndpoint(ministackEndpoint),
	)
	if err != nil {
		t.Fatalf("loading AWS config: %v", err)
	}
	return cfg
}

func buildTestTarball(t *testing.T) []byte {
	t.Helper()
	entries, err := os.ReadDir(sqsTestDataDir)
	if err != nil {
		t.Fatalf("reading test data dir: %v", err)
	}

	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)
	count := 0

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".stat") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(sqsTestDataDir, entry.Name()))
		if err != nil {
			t.Fatalf("reading %s: %v", entry.Name(), err)
		}
		tw.WriteHeader(&tar.Header{
			Name:     entry.Name(),
			Mode:     0o644,
			Size:     int64(len(data)),
			Typeflag: tar.TypeReg,
		})
		tw.Write(data)
		count++
	}
	tw.Close()
	gw.Close()

	if count == 0 {
		t.Fatal("no .stat files found in test data")
	}
	t.Logf("built tarball: %d stat files, %d bytes", count, buf.Len())
	return buf.Bytes()
}

func TestSQSWorker_EndToEnd(t *testing.T) {
	requireMiniStack(t)

	cfg := newTestAWSConfig(t)
	ctx := context.Background()

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	sqsClient := sqs.NewFromConfig(cfg)

	// Upload tarball to S3
	s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(sqsTestBucket),
	})
	tarball := buildTestTarball(t)
	_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(sqsTestBucket),
		Key:               aws.String(sqsTestKey),
		Body:              bytes.NewReader(tarball),
		ContentType:       aws.String("application/gzip"),
		ChecksumAlgorithm: "SHA256",
	})
	if err != nil {
		t.Fatalf("uploading tarball: %v", err)
	}

	// Create SQS queue
	createOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("sqsworker-integration-test"),
	})
	if err != nil {
		t.Fatalf("creating SQS queue: %v", err)
	}
	queueURL := *createOut.QueueUrl

	// Send S3 event notification to SQS
	s3Event := map[string]interface{}{
		"Records": []map[string]interface{}{
			{
				"eventVersion": "2.4",
				"eventSource":  "aws:s3",
				"eventName":    "ObjectCreated:Put",
				"s3": map[string]interface{}{
					"bucket": map[string]string{"name": sqsTestBucket},
					"object": map[string]interface{}{"key": sqsTestKey, "size": len(tarball)},
				},
			},
		},
	}
	eventJSON, _ := json.Marshal(s3Event)

	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(eventJSON)),
	})
	if err != nil {
		t.Fatalf("sending SQS message: %v", err)
	}

	// Set up state for CREATE_JSON_DOC_ARCHIVE mode (no DB needed)
	state.LoadSpec, _ = core.ParseLoadSpec("../../load_spec.json")
	state.LoadSpec.RunMode = "CREATE_JSON_DOC_ARCHIVE"
	state.LoadSpec.JsonArchiveFilePathAndPrefix = filepath.Join(t.TempDir(), "sqstest_")
	state.LoadSpec.DatasetName = "SQSTEST"

	// Receive message from SQS
	recvOut, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     5,
	})
	if err != nil {
		t.Fatalf("receiving SQS message: %v", err)
	}
	if len(recvOut.Messages) == 0 {
		t.Fatal("expected 1 message from SQS, got 0")
	}

	msg := recvOut.Messages[0]

	// Process the message using the worker's handleMessage function
	err = handleMessage(ctx, sqsClient, s3Client, queueURL, *msg.Body, *msg.ReceiptHandle)
	if err != nil {
		t.Fatalf("handleMessage failed: %v", err)
	}

	// Verify the message was deleted from the queue
	recvOut2, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     1,
	})
	if err != nil {
		t.Fatalf("second receive failed: %v", err)
	}
	if len(recvOut2.Messages) != 0 {
		t.Error("expected queue to be empty after successful processing")
	}

	t.Log("SQS worker end-to-end test passed: message received, tarball streamed from S3, processed, message deleted")
}
