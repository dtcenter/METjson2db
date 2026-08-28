# AGENTS.md — METjson2db

## Project Overview

METjson2db is a Go tool that parses MET (Model Evaluation Tools) `.stat` files into JSON documents and loads them into Couchbase. It supports two entrypoints: a **CLI** for local stat files and an **SQS worker** that streams tarballs from S3. Both share a common pipeline via the `StorageProvider` interface. It is part of the DTC verification ecosystem (`github.com/dtcenter/METjson2db`).

See [docs/architecture.md](docs/architecture.md) for data flow diagrams and package descriptions. See [docs/dev-guide.md](docs/dev-guide.md) for test and build instructions.

## Build & Run

```bash
# Build both binaries (output to gitignored build/ directory)
go build -o build/metjson2db ./cmd/metjson2db
go build -o build/sqsworker ./cmd/sqsworker

# Run unit tests
go test ./...

# Run S3 integration tests (requires MiniStack: docker run -d -p 4566:4566 ministackorg/ministack)
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test go test -tags integration ./pkg/storage/...

# Run Couchbase integration tests (requires ~/credentials)
go test -tags integration ./pkg/black_box_tests/...

# Docker
docker build -t sqsworker .
```

See [docs/dev-guide.md](docs/dev-guide.md) for full details on running tests.

## Project Structure

- `cmd/metjson2db/` — CLI entry point. Flag parsing and file discovery, creates a `LocalProvider`.
- `cmd/sqsworker/` — SQS worker entry point. Polls SQS for S3 event notifications, creates `S3TarballProvider` instances. Initializes OpenTelemetry SDK and instruments the processing loop.
- `pkg/storage/` — `StorageProvider` interface and implementations (`LocalProvider`, `S3TarballProvider`, S3 event parser). This is the abstraction layer between file sources and the processing pipeline.
- `pkg/core/` — Core pipeline: `ProcessFromProvider` orchestrates workers, `parseStatFileContent` parses via `io.Reader`, flushing to DB/disk. `ConnectDbIfNeeded` establishes `state.DbConn` once at startup (gated on run modes that actually need Couchbase); both entrypoints call it right after loading credentials.
- `pkg/async/` — Goroutine workers for concurrent DB upserts and merge-doc fetching.
- `pkg/telemetry/` — OpenTelemetry metric instruments and tracer. Depends only on the OTel API (no SDK). Imported by all instrumented packages.
- `pkg/state/` — Global shared state (maps, mutexes, channels). **Caution**: tightly coupled; read before modifying. `state.DbConn` is the single process-lifetime Couchbase connection (see Concurrency below) — like `state.Credentials`, it is **not** reset by `state.StateReset()`.
- `pkg/types/` — All shared data structures (`LoadSpec`, `Credentials`, `CbConnection`, etc.).
- `pkg/utils/` — DB connection helpers (`GetDbConnection` — call once via `core.ConnectDbIfNeeded()`, not per operation), query execution, JSON formatting.
- `pkg/metadataUpdate/` — `METADATA_UPDATE` run mode: queries DB and builds aggregate metadata documents.
- `pkg/black_box_tests/` — Couchbase integration tests (build tag: `integration`).
- `internal/otel/` — OpenTelemetry SDK initialization (TracerProvider, MeterProvider, LoggerProvider) and slog fan-out bridge. Application-specific; not importable by external modules.
- `sqlTemplates/` — Parameterized SQL++ templates used by metadata update.
- `test_data/` — Sample `.stat` files for testing.

## Key Configuration Files

- **`load_spec.json`** — Runtime config: run mode, threading, folder templates, dataset name. This is checked into the repo but contains environment-specific paths that may need editing.
- **`~/credentials`** — Couchbase connection credentials (YAML). **Never commit this file.** Use `credentials.template` as a reference.

## Coding Conventions

### Follow

- Go standard project layout: `cmd/` for binaries, `pkg/` for library code.
- Use `log/slog` for all logging (JSON handler, structured fields).
- Structured error returns — propagate errors to callers via return values.
- Use `filepath.Join()` for path construction, not string concatenation.
- Run `golangci-lint fmt ./...`, `golangci-lint run ./...`, `go test ./...` after changes.
- Run `go mod tidy` after adding dependencies.
- Integration tests must use the `//go:build integration` build tag.
- Use CamelCase for exported Go identifiers (not snake_case).
- Make sure to update relevant docs with each change.
- Follow common Go best practices.
- All new code should have unit test coverage.
- Run `golangci-lint` to check code quality & formatting.
- Group changes into logical commits to avoid massive PRs

### Avoid

- Adding `init()` functions unless absolutely necessary for package-level setup.
- Using `log.Fatal()` or `os.Exit()` in library code (`pkg/`).
- Creating new Couchbase connections per operation — connections should be reused.
- Modifying `pkg/state/` globals without holding the appropriate mutex.
- Committing credentials, passwords, or connection strings.
- Doing numerous changes in "one shot" - resulting in single commits with massive changsets

## External Dependencies

- **`github.com/dtcenter/METstat2json`** — Core parsing library. Defines line-type schemas per MET version. Changes to stat file format require updates in that repo first.
- **`github.com/couchbase/gocb/v2`** — Couchbase Go SDK.
- **`github.com/aws/aws-sdk-go-v2`** — AWS SDK (v2). Used by `S3TarballProvider` (S3 streaming) and the SQS worker (message polling). Auth via default credential chain.
- **`go.opentelemetry.io/otel`** — OpenTelemetry API and SDK. Metrics, traces, and logs exported via OTLP gRPC. See [docs/observability.md](docs/observability.md).

## Run Modes

| Mode                      | Needs DB? | Description                                        |
| ------------------------- | --------- | -------------------------------------------------- |
| `DIRECT_LOAD_TO_DB`       | Yes       | Parse and upsert documents to Couchbase            |
| `CREATE_JSON_DOC_ARCHIVE` | No        | Parse and write compressed JSON to disk            |
| `METADATA_UPDATE`         | Yes       | Aggregate existing DB data into metadata documents |

## Concurrency

The DB upload pipeline uses goroutines and buffered channels in a fan-out pattern. Key settings in `load_spec.json`:

- `threadsDbUpload` — Number of DB writer goroutines (default 32)
- `threadsMergeDocFetch` — Number of merge-fetch goroutines (default 4)
- `channelBufferSizeNumberOfDocs` — Channel buffer size (default 1024)

End-of-stream is signaled via sentinel "endMarker" values. Shared maps are protected by `sync.RWMutex` in `pkg/state/`.

All of these worker goroutines share a single Couchbase connection (`state.DbConn`), established once at process startup via `core.ConnectDbIfNeeded()` — not one connection per worker. A `*gocb.Cluster` and its `Collection`/`Bucket`/`Scope` children are documented as safe for concurrent use from many goroutines, which is exactly what this fan-out relies on. If Couchbase becomes unreachable mid-process, individual `Upsert`/`Get` calls on the shared connection surface that as a per-operation error (gocb's own retry/pool logic handles the transport layer) rather than requiring any reconnect logic here.

## Known Technical Debt

1. **Global mutable state** in `pkg/state/` — prevents concurrent pipeline runs and complicates testing.
2. **Mixed logging** — `metadataUpdate` uses `log` instead of `slog`.
3. **SQL template string replacement** — not parameterized; safe today (internal values) but fragile.
4. **Minimal test coverage in core** — one unit test, one integration test for the core pipeline. `pkg/storage/` has 90%+ coverage.
5. Files are output without proper file endings. When creating archives, the files should be tar.gz files.
6. **Context not propagated to DB calls** — Async workers now receive `context.Context`, but the underlying `gocb` Upsert and Get calls still use `nil` options (no context). Passing context to Couchbase SDK calls would enable timeout propagation and trace-linked DB spans.
7. **Merge-fetch failure can silently overwrite/lose previously-persisted data** — `Collection.Upsert` is a full-document replace, not a partial patch. If `MergeDbDocFetchAsync` fails to fetch the existing document (e.g. a connectivity blip), `FlushToDbAsync` proceeds to upsert the incoming document unmerged, which can destroy fields accumulated from earlier deliveries. Not currently detected or logged as such (in progress: will be logged explicitly, not prevented — see `docs/plan/couchbase-upsert-reliability.md` for the tradeoff and why a real fix needs a bigger redesign, e.g. sub-document `MutateIn`).
8. **`pkg/black_box_tests/merge_test.go` doesn't compile under `-tags integration`** — references `state.Conf`, which doesn't exist (field is `state.LoadSpec`). Pre-existing, unrelated to recent changes; `go vet ./...` (no tag) is unaffected.
