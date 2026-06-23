# Developer Guide

## Prerequisites

- Go 1.24+
- Docker (for MiniStack and container builds)

## Build

Binaries go in the `build/` directory (gitignored):

```bash
go build -o build/metjson2db ./cmd/metjson2db
go build -o build/sqsworker ./cmd/sqsworker
```

## Tests

### Unit tests

Unit tests have no external dependencies and run with the standard Go test command:

```bash
go test ./...
```

This runs all tests **except** those gated behind the `integration` build tag.

### Integration tests

Integration tests use the `//go:build integration` build tag, so `go test ./...` never runs them. You must opt in with `-tags integration`.

There are two independent groups of integration tests, each with different infrastructure requirements. You can run them separately or together.

#### S3 storage tests (`pkg/storage/`)

These tests build a tarball from `test_data/`, upload it to a local S3-compatible endpoint, and stream it back through `S3TarballProvider`. They require a running [MiniStack](https://github.com/ministackorg/ministack) instance.

**Start MiniStack:**

```bash
docker run --name ministack -d -p 4566:4566 ministackorg/ministack
```

**Run the tests:**

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  go test -tags integration ./pkg/storage/ -v
```

The test checks `http://localhost:4566/_ministack/health` before doing anything and will fail with setup instructions if MiniStack isn't reachable.

**Stop MiniStack when done:**

```bash
docker rm -f ministack
```

#### SQS worker tests (`cmd/sqsworker/`)

These tests exercise the full SQS message flow: build a tarball, upload to S3, send an S3 event notification to SQS, receive the message, process it through `handleMessage`, and verify the message is deleted from the queue. They also require MiniStack.

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  go test -tags integration ./cmd/sqsworker/ -v
```

#### Couchbase merge tests (`pkg/black_box_tests/`)

These tests exercise the full parse-and-upsert pipeline against a live Couchbase cluster. They require a `~/credentials` file with valid connection details (see `credentials.template`).

```bash
go test -tags integration ./pkg/black_box_tests/ -v
```

#### Running all integration tests at once

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  go test -tags integration ./... -v
```

This requires both MiniStack (for S3/SQS tests) and Couchbase (for merge tests) to be available.

### Coverage

```bash
# Unit test coverage
go test ./... -cover

# Coverage for a specific package
go test ./pkg/storage/ -cover

# HTML coverage report
go test ./pkg/storage/ -coverprofile=coverage.out
go tool cover -html=coverage.out
```

## CLI smoke test

To verify the parsing pipeline end-to-end without a database, use `CREATE_JSON_DOC_ARCHIVE` mode. This parses stat files through the full `StorageProvider` → `ProcessFromProvider` pipeline and writes compressed JSON to disk:

```bash
go run ./cmd/metjson2db \
  -f test_data/grid_stat_GFS_TMP_vs_ANLYS_TMP_P1000_anom_120000L_20240203_120000V.stat \
  -m CREATE_JSON_DOC_ARCHIVE \
  -l load_spec.json \
  -d SMOKETEST
```

Note: the default `jsonArchiveFilePathAndPrefix` in `load_spec.json` points to `/scratch/`, which likely doesn't exist locally. Either create that directory or edit the field to a valid path like `/tmp/metjson2db_out_`.

## Docker

```bash
docker build -t metjson2db .
```

The Dockerfile builds both `metjson2db` and `sqsworker` binaries. Since there is no hardcoded entrypoint, specify the binary when running:

```bash
# CLI
docker run metjson2db /app/metjson2db -f /path/to/file.stat -m CREATE_JSON_DOC_ARCHIVE

# SQS worker
docker run -e SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789/my-queue \
  metjson2db /app/sqsworker -l /app/load_spec.json
```
