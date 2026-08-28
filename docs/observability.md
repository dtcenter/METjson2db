# Observability

The SQS worker is instrumented with OpenTelemetry to export metrics, traces, and structured logs via OTLP gRPC. This document covers the instrumentation architecture, available signals, and deployment configuration.

## Architecture

```
pkg/telemetry/          OTel API layer (metric instruments, tracer)
                        Imported by all instrumented packages.
                        No SDK dependency — keeps compile graph light.

internal/otel/          OTel SDK layer (application-specific)
  otel.go               InitOTel() — builds providers, registers exporters
  slogbridge.go         Fan-out slog.Handler: stdout JSON + OTel log bridge
```

`pkg/telemetry/` declares all instruments as package-level variables initialized via `init()` with no-op defaults from the global OTel provider. When `InitOTel()` is called at startup, it replaces the global provider with a real SDK provider and re-initializes the instruments.

This means any package can safely call `telemetry.LinesParsed.Add(ctx, 1)` even in tests where `InitOTel()` was never called — the no-op instruments accept and discard the data.

## Signals

### Traces

Each SQS message produces a trace with the following span hierarchy:

```
process_sqs_message (root)
├── process_s3_record (per S3 record)
│   ├── S3.GetObject (auto-instrumented by otelaws)
│   └── parse_stat_file (per file in tarball)
├── SQS.DeleteMessage (auto-instrumented by otelaws)
└── SQS.ChangeMessageVisibility (auto-instrumented by otelaws, per heartbeat)
```

AWS SDK calls (S3, SQS) are automatically traced via [`otelaws`](https://pkg.go.dev/go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws) middleware, which captures operation name, HTTP status, retries, and request ID. Application-level spans (`process_sqs_message`, `process_s3_record`, `parse_stat_file`) provide business context.

Span attributes include `s3.bucket`, `s3.key`, and `file.name` where applicable.

### Metrics

All metrics use the `metjson2db.` namespace prefix.

#### Health signals

| Metric                                           | Type          | Attributes | Description                                                  |
| ------------------------------------------------ | ------------- | ---------- | ------------------------------------------------------------ |
| `metjson2db.messages.received`                   | Counter       | —          | SQS messages received                                        |
| `metjson2db.messages.processed`                  | Counter       | `status`   | Messages processed (success/error)                           |
| `metjson2db.messages.deleted`                    | Counter       | —          | Messages deleted from queue                                  |
| `metjson2db.files.processed`                     | Counter       | `status`   | Stat files parsed (success/error)                            |
| `metjson2db.documents.upserted`                  | Counter       | `status`   | Couchbase upserts (success/error)                            |
| `metjson2db.lines.parsed`                        | Counter       | —          | Data lines parsed                                            |
| `metjson2db.message.processing.duration`         | Histogram (s) | —          | End-to-end message processing time                           |
| `metjson2db.s3.download.duration`                | Histogram (s) | —          | Time to fully stream and extract an S3 tarball               |
| `metjson2db.db.upsert.duration`                  | Histogram (s) | —          | Individual Couchbase upsert latency                          |
| `metjson2db.sqs.empty_receives`                  | Counter       | —          | Long-poll responses with no messages                         |
| `metjson2db.sqs.visibility_heartbeat.extensions` | Counter       | `status`   | Heartbeat extension attempts                                 |
| `metjson2db.db.connection_errors`                | Counter       | —          | Couchbase connectivity errors (timeout/canceled/unavailable) |

#### Bad data signals

| Metric                                       | Type    | Attributes | Description                       |
| -------------------------------------------- | ------- | ---------- | --------------------------------- |
| `metjson2db.s3_events.filtered`              | Counter | `reason`   | Events filtered before processing |
| `metjson2db.tarball.extraction_errors`       | Counter | —          | Corrupt gzip/tar errors           |
| `metjson2db.stat_file.parse_errors`          | Counter | —          | Line-level parse failures         |
| `metjson2db.lines.skipped`                   | Counter | —          | Empty lines skipped               |
| `metjson2db.documents.merged`                | Counter | —          | Documents merged with existing    |
| `metjson2db.documents.missing_external_refs` | Counter | —          | Missing external doc lookups      |

#### Attribute values

| Attribute | Values                                  |
| --------- | --------------------------------------- |
| `status`  | `success`, `error`                      |
| `reason`  | `not_object_created`, `malformed_event` |

All attributes are low-cardinality enumerations to keep metric series counts bounded.

#### Go runtime metrics

The `go.opentelemetry.io/contrib/instrumentation/runtime` collector exposes standard Go runtime metrics (GC pauses, goroutine count, memory allocation) under the `process.runtime.go.*` namespace.

### Logs

Structured logs (via `log/slog`) are exported through a fan-out handler:

1. **stdout** — JSON format with source location (for `kubectl logs` / local dev)
2. **OTLP** — Sent to the OTel Collector via the log bridge

When a log call uses `slog.InfoContext(ctx, ...)` and the context carries an active span, the log record is automatically annotated with `TraceID` and `SpanID` for correlation in your observability backend.

## Deployment Configuration

The OTel SDK reads standard environment variables — no application-specific configuration is needed.

| Variable                      | Default                | Description                      |
| ----------------------------- | ---------------------- | -------------------------------- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317`       | Collector gRPC endpoint          |
| `OTEL_SERVICE_NAME`           | `metjson2db-sqsworker` | Service name in resource         |
| `OTEL_RESOURCE_ATTRIBUTES`    | —                      | Additional resource attributes   |
| `OTEL_METRICS_EXPORTER`       | `otlp`                 | Set to `none` to disable metrics |
| `OTEL_TRACES_EXPORTER`        | `otlp`                 | Set to `none` to disable traces  |
| `OTEL_LOGS_EXPORTER`          | `otlp`                 | Set to `none` to disable logs    |

### Kubernetes with OpenTelemetry Operator

The OpenTelemetry Operator manages collector injection. A typical setup:

1. Deploy the `OpenTelemetryCollector` CR (sidecar or DaemonSet mode)
2. Annotate the Deployment/ScaledJob pod template:

   ```yaml
   sidecar.opentelemetry.io/inject: "true"
   ```

3. The operator injects `OTEL_EXPORTER_OTLP_ENDPOINT` automatically

No code changes are required — the SDK picks up the endpoint from the environment.

### KEDA ScaledJob considerations

When running as a KEDA ScaledJob, pods are ephemeral and may process only a few messages before terminating. The implementation accounts for this:

- **PeriodicReader interval**: 30 seconds (short enough to avoid data loss in short-lived pods)
- **Shutdown flush**: 5-second timeout after `pollLoop` returns, ensuring all buffered telemetry is exported before the process exits
- **Graceful shutdown**: `SIGTERM` cancels the poll loop immediately; in-flight messages complete via `context.WithoutCancel`; OTel shutdown runs last

### Local development (no collector)

When no collector is reachable, the SDK initializes normally but export attempts fail silently. The application runs without observability overhead. Logs still appear on stdout.

To disable OTel entirely (zero gRPC connection attempts):

```bash
OTEL_METRICS_EXPORTER=none OTEL_TRACES_EXPORTER=none OTEL_LOGS_EXPORTER=none \
  go run ./cmd/sqsworker ...
```

## Adding new instrumentation

When adding new processing paths or error conditions:

1. Declare the instrument in `pkg/telemetry/metrics.go` inside `InitMetrics()`
2. Use `telemetry.<Instrument>.Add(ctx, 1)` or `.Record(ctx, value)` at the call site
3. Prefer `slog.InfoContext(ctx, ...)` over `slog.Info(...)` in functions that have a context to enable trace correlation
4. Keep attribute cardinality low — use bounded enumerations, never unbounded values like filenames or document IDs
