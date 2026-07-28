package telemetry

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var Tracer trace.Tracer = otel.Tracer("metjson2db")

const (
	SpanProcessMessage = "process_sqs_message"
	SpanParseS3Event   = "parse_s3_event"
	SpanProcessRecord  = "process_s3_record"
	SpanS3Download     = "s3_download"
	SpanParseStatFile  = "parse_stat_file"
	SpanDeleteMessage  = "delete_sqs_message"
)

func WithFileAttribute(name string) trace.SpanStartOption {
	return trace.WithAttributes(attribute.String("file.name", name))
}
