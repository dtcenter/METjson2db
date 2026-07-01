package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/dtcenter/METjson2db/pkg/core"
	"github.com/dtcenter/METjson2db/pkg/state"
	"github.com/dtcenter/METjson2db/pkg/storage"
)

func main() {
	home, _ := os.UserHomeDir()

	var credentialsFilePath string
	flag.StringVar(&credentialsFilePath, "c", home+"/credentials", "path to credentials file")

	var loadSpecFilePath string
	flag.StringVar(&loadSpecFilePath, "l", "./load_spec.json", "path to load_spec.json")

	var awsEndpoint string
	flag.StringVar(&awsEndpoint, "endpoint", "", "AWS endpoint override (e.g. http://localhost:4566 for MiniStack)")

	flag.Parse()

	queueURL := os.Getenv("SQS_QUEUE_URL")
	if queueURL == "" {
		slog.Error("SQS_QUEUE_URL environment variable is required")
		os.Exit(1)
	}

	var err error
	state.LoadSpec, err = core.ParseLoadSpec(loadSpecFilePath)
	if err != nil {
		slog.Error("unable to parse load_spec", "error", err)
		os.Exit(1)
	}
	state.LoadSpec.RunMode = "DIRECT_LOAD_TO_DB"

	level := slog.LevelInfo
	switch state.LoadSpec.LogLevel {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     level,
	}))
	slog.SetDefault(logger)

	state.Credentials = core.GetCredentials(credentialsFilePath)
	if len(state.LoadSpec.TargetCollection) > 0 {
		state.Credentials.Cb_collection = state.LoadSpec.TargetCollection
	}
	slog.Info("sqsworker starting",
		"queue", queueURL,
		"db", fmt.Sprintf("%s.%s.%s", state.Credentials.Cb_bucket, state.Credentials.Cb_scope, state.Credentials.Cb_collection),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var cfgOpts []func(*config.LoadOptions) error
	cfgOpts = append(cfgOpts, config.WithRegion(envOrDefault("AWS_REGION", "us-east-1")))
	if awsEndpoint != "" {
		cfgOpts = append(cfgOpts, config.WithBaseEndpoint(awsEndpoint))
	}

	cfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
	if err != nil {
		slog.Error("loading AWS config", "error", err)
		os.Exit(1)
	}

	sqsClient := sqs.NewFromConfig(cfg)
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = awsEndpoint != ""
	})

	slog.Info("sqsworker ready, polling for messages")
	pollLoop(ctx, sqsClient, s3Client, queueURL)
	slog.Info("sqsworker shutdown complete")
}

// pollLoop Uses a 20 second long-poll looking for messages on the SQS queue
func pollLoop(ctx context.Context, sqsClient *sqs.Client, s3Client *s3.Client, queueURL string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     20,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("receiving SQS message", "error", err)
			continue
		}

		for _, msg := range out.Messages {
			if err := handleMessage(ctx, sqsClient, s3Client, queueURL, *msg.Body, *msg.ReceiptHandle); err != nil {
				slog.Error("processing message failed, leaving in queue for retry",
					"messageId", *msg.MessageId,
					"error", err,
				)
			}
		}
	}
}

func handleMessage(ctx context.Context, sqsClient *sqs.Client, s3Client *s3.Client, queueURL, body, receiptHandle string) error {
	event, err := storage.ParseS3Event(body)
	if err != nil {
		return fmt.Errorf("parsing S3 event: %w", err)
	}

	visibilityTimeout, err := fetchQueueVisibilityTimeout(ctx, sqsClient, queueURL)
	if err != nil {
		slog.Warn("failed to fetch queue visibility timeout, using default 30s", "error", err)
		visibilityTimeout = 30
	}

	stopHeartbeat := startVisibilityHeartbeat(ctx, sqsClient, queueURL, receiptHandle, visibilityTimeout)
	defer stopHeartbeat()

	for _, record := range event.Records {
		bucket := record.S3.Bucket.Name
		key := record.S3.Object.Key

		slog.Info("processing tarball", "bucket", bucket, "key", key)

		provider := storage.NewS3TarballProvider(s3Client, bucket, key)
		if err := core.ProcessFromProvider(ctx, provider, nil); err != nil {
			return fmt.Errorf("processing s3://%s/%s: %w", bucket, key, err)
		}

		slog.Info("tarball processed successfully", "bucket", bucket, "key", key)
	}

	_, err = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		return fmt.Errorf("deleting SQS message: %w", err)
	}

	return nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
