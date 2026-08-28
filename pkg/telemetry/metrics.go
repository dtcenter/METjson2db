package telemetry

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	StatusSuccess = metric.WithAttributes(attribute.String("status", "success"))
	StatusError   = metric.WithAttributes(attribute.String("status", "error"))

	FilterReasonNotCreated = metric.WithAttributes(attribute.String("reason", "not_object_created"))
	FilterReasonMalformed  = metric.WithAttributes(attribute.String("reason", "malformed_event"))
)

func init() {
	InitMetrics(otel.Meter("metjson2db"))
}

var (
	// Health signals
	MessagesReceived          metric.Int64Counter
	MessagesProcessed         metric.Int64Counter
	MessagesDeleted           metric.Int64Counter
	FilesProcessed            metric.Int64Counter
	DocumentsUpserted         metric.Int64Counter
	LinesParsed               metric.Int64Counter
	MessageProcessingDuration metric.Float64Histogram
	S3DownloadDuration        metric.Float64Histogram
	DbUpsertDuration          metric.Float64Histogram
	SQSEmptyReceives          metric.Int64Counter
	VisibilityHeartbeatExts   metric.Int64Counter
	DbConnectionErrors        metric.Int64Counter

	// Bad data signals
	S3EventsFiltered        metric.Int64Counter
	TarballExtractionErrors metric.Int64Counter
	StatFileParseErrors     metric.Int64Counter
	LinesSkipped            metric.Int64Counter
	DocumentsMerged         metric.Int64Counter
	MissingExternalDocRefs  metric.Int64Counter
)

func InitMetrics(m metric.Meter) {
	MessagesReceived, _ = m.Int64Counter("metjson2db.messages.received",
		metric.WithDescription("Total SQS messages received"))

	MessagesProcessed, _ = m.Int64Counter("metjson2db.messages.processed",
		metric.WithDescription("Total SQS messages processed"),
		metric.WithUnit("{message}"))

	MessagesDeleted, _ = m.Int64Counter("metjson2db.messages.deleted",
		metric.WithDescription("Total SQS messages successfully deleted"))

	FilesProcessed, _ = m.Int64Counter("metjson2db.files.processed",
		metric.WithDescription("Total stat files processed from tarballs"),
		metric.WithUnit("{file}"))

	DocumentsUpserted, _ = m.Int64Counter("metjson2db.documents.upserted",
		metric.WithDescription("Total documents upserted to Couchbase"),
		metric.WithUnit("{document}"))

	LinesParsed, _ = m.Int64Counter("metjson2db.lines.parsed",
		metric.WithDescription("Total data lines parsed from stat files"))

	MessageProcessingDuration, _ = m.Float64Histogram("metjson2db.message.processing.duration",
		metric.WithDescription("Duration to process an entire SQS message"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300))

	S3DownloadDuration, _ = m.Float64Histogram("metjson2db.s3.download.duration",
		metric.WithDescription("Duration to fully stream and extract an S3 tarball"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10))

	DbUpsertDuration, _ = m.Float64Histogram("metjson2db.db.upsert.duration",
		metric.WithDescription("Duration of individual Couchbase upsert operations"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1))

	SQSEmptyReceives, _ = m.Int64Counter("metjson2db.sqs.empty_receives",
		metric.WithDescription("Total SQS long-poll responses with no messages"))

	VisibilityHeartbeatExts, _ = m.Int64Counter("metjson2db.sqs.visibility_heartbeat.extensions",
		metric.WithDescription("Total SQS visibility timeout extension attempts"))

	DbConnectionErrors, _ = m.Int64Counter("metjson2db.db.connection_errors",
		metric.WithDescription("Total Couchbase connection errors"))

	S3EventsFiltered, _ = m.Int64Counter("metjson2db.s3_events.filtered",
		metric.WithDescription("Total S3 events filtered out before processing"))

	TarballExtractionErrors, _ = m.Int64Counter("metjson2db.tarball.extraction_errors",
		metric.WithDescription("Total errors extracting entries from tarballs"))

	StatFileParseErrors, _ = m.Int64Counter("metjson2db.stat_file.parse_errors",
		metric.WithDescription("Total errors parsing individual lines in stat files"))

	LinesSkipped, _ = m.Int64Counter("metjson2db.lines.skipped",
		metric.WithDescription("Total lines skipped during parsing"))

	DocumentsMerged, _ = m.Int64Counter("metjson2db.documents.merged",
		metric.WithDescription("Total documents merged with existing DB documents"))

	MissingExternalDocRefs, _ = m.Int64Counter("metjson2db.documents.missing_external_refs",
		metric.WithDescription("Total missing external document reference lookups"))
}
