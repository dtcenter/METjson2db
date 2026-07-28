package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	intotel "github.com/dtcenter/METjson2db/internal/otel"
	"github.com/dtcenter/METjson2db/pkg/core"
	"github.com/dtcenter/METjson2db/pkg/state"
	"github.com/dtcenter/METjson2db/pkg/storage"
	"github.com/dtcenter/METjson2db/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func main() {
	home, _ := os.UserHomeDir()

	var credentialsFilePath string
	flag.StringVar(&credentialsFilePath, "c", home+"/credentials", "path to credentials file")

	var loadSpecFilePath string
	flag.StringVar(&loadSpecFilePath, "l", "./load_spec.json", "path to load_spec.json")

	var awsEndpoint string
	flag.StringVar(&awsEndpoint, "endpoint", "", "AWS endpoint override (e.g. http://localhost:4566 for MiniStack)")

	var jsonOutputPrefix string
	flag.StringVar(&jsonOutputPrefix, "json-output", "", "write parsed docs to compressed JSON files with this path prefix instead of loading to DB (e.g. /tmp/metjson2db_out_)")

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
	if jsonOutputPrefix != "" {
		state.LoadSpec.RunMode = "CREATE_JSON_DOC_ARCHIVE"
		state.LoadSpec.JsonArchiveFilePathAndPrefix = jsonOutputPrefix
	} else {
		state.LoadSpec.RunMode = "DIRECT_LOAD_TO_DB"
	}

	level := slog.LevelInfo
	switch state.LoadSpec.LogLevel {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	otelShutdown, err := intotel.InitOTel(ctx)
	if err != nil {
		slog.Error("initializing OpenTelemetry", "error", err)
		os.Exit(1)
	}

	logger := slog.New(intotel.NewFanoutHandler(level))
	slog.SetDefault(logger)

	state.Credentials = core.GetCredentials(credentialsFilePath)
	if len(state.LoadSpec.TargetCollection) > 0 {
		state.Credentials.Cb_collection = state.LoadSpec.TargetCollection
	}
	slog.Info("sqsworker starting",
		"queue", queueURL,
		"db", fmt.Sprintf("%s.%s.%s", state.Credentials.Cb_bucket, state.Credentials.Cb_scope, state.Credentials.Cb_collection),
	)

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

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := otelShutdown(shutdownCtx); err != nil {
		slog.Error("otel shutdown", "error", err)
	}

	slog.Info("sqsworker shutdown complete")
}

// sqsHandler combines the SQS operations needed by handleMessage.
// *sqs.Client satisfies this interface.
type sqsHandler interface {
	sqsAttributeGetter
	sqsVisibilityChanger
	sqsMessageDeleter
}

// pollLoop polls for messages to process on the SQS queue
// It uses a standard long-poll
func pollLoop(ctx context.Context, sqsClient *sqs.Client, s3Client *s3.Client, queueURL string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(queueURL),
			// Must stay 1: ProcessFromProvider resets global state in pkg/state/ and is not safe for concurrent runs.
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

		if len(out.Messages) == 0 {
			telemetry.SQSEmptyReceives.Add(ctx, 1)
			continue
		}

		for _, msg := range out.Messages {
			if msg.Body == nil || msg.ReceiptHandle == nil {
				slog.Error("received SQS message missing body or receipt handle", "messageId", aws.ToString(msg.MessageId))
				continue
			}
			// Use a context detached from the signal so in-flight work completes gracefully.
			telemetry.MessagesReceived.Add(ctx, 1)
			msgCtx := context.WithoutCancel(ctx)
			if err := handleMessage(msgCtx, sqsClient, s3Client, queueURL, aws.ToString(msg.Body), aws.ToString(msg.ReceiptHandle)); err != nil {
				telemetry.MessagesProcessed.Add(ctx, 1, telemetry.StatusError)
				slog.Error("processing message failed, leaving in queue for retry",
					"messageId", aws.ToString(msg.MessageId),
					"error", err,
				)
			} else {
				telemetry.MessagesProcessed.Add(ctx, 1, telemetry.StatusSuccess)
			}
		}
	}
}

func handleMessage(ctx context.Context, sqsClient sqsHandler, s3Client *s3.Client, queueURL, body, receiptHandle string) error {
	ctx, span := telemetry.Tracer.Start(ctx, telemetry.SpanProcessMessage)
	defer span.End()
	start := time.Now()
	defer func() {
		telemetry.MessageProcessingDuration.Record(ctx, time.Since(start).Seconds())
	}()

	event, err := storage.ParseS3Event(body)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		telemetry.S3EventsFiltered.Add(ctx, 1, telemetry.FilterReasonMalformed)
		return fmt.Errorf("parsing S3 event: %w", err)
	}

	visibilityTimeout, err := fetchQueueVisibilityTimeout(ctx, sqsClient, queueURL)
	if err != nil {
		slog.WarnContext(ctx, "failed to fetch queue visibility timeout, using default 30s", "error", err)
		visibilityTimeout = 30
	}

	stopHeartbeat := startVisibilityHeartbeat(ctx, sqsClient, queueURL, receiptHandle, visibilityTimeout)
	defer stopHeartbeat()

	// Records in an S3 event are processed sequentially. If a later record fails the message
	// is retried and earlier records are reprocessed — safe because DB upserts are idempotent
	// when overWriteData is true in load_spec.json.
	for _, record := range event.Records {
		if !strings.HasPrefix(record.EventName, "ObjectCreated:") {
			telemetry.S3EventsFiltered.Add(ctx, 1, telemetry.FilterReasonNotCreated)
			slog.InfoContext(ctx, "skipping non-creation S3 event", "eventName", record.EventName)
			continue
		}

		bucket := record.S3.Bucket.Name
		key := record.S3.Object.Key

		ctx, recordSpan := telemetry.Tracer.Start(ctx, telemetry.SpanProcessRecord,
			trace.WithAttributes(
				attribute.String("s3.bucket", bucket),
				attribute.String("s3.key", key),
			))

		slog.InfoContext(ctx, "processing tarball", "bucket", bucket, "key", key)

		provider := storage.NewS3TarballProvider(s3Client, bucket, key)
		if err := core.ProcessFromProvider(ctx, provider, nil); err != nil {
			recordSpan.SetStatus(codes.Error, err.Error())
			recordSpan.End()
			span.SetStatus(codes.Error, err.Error())
			return fmt.Errorf("processing s3://%s/%s: %w", bucket, key, err)
		}

		slog.InfoContext(ctx, "tarball processed successfully", "bucket", bucket, "key", key)
		recordSpan.End()
	}

	_, deleteSpan := telemetry.Tracer.Start(ctx, telemetry.SpanDeleteMessage)
	_, err = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	deleteSpan.End()
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("deleting SQS message: %w", err)
	}

	telemetry.MessagesDeleted.Add(ctx, 1)
	return nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
