# Developer Guide

## Prerequisites

- Go 1.25+
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

These tests exercise the full parse-and-upsert pipeline against a live Couchbase cluster: they upload real stat files through `ProcessInputFiles` with multiple concurrent DB-upload and merge-fetch goroutines, so this is also the best local way to validate any change to `pkg/async/` or the shared-connection code in `pkg/utils/db-utils.go`.

**⚠️ This test runs a raw `DELETE FROM <bucket>.<scope>.<collection>` against whatever credentials it's given.** Never point it at a shared or production cluster — use a disposable local Couchbase instance, and use a **separate** credentials file from your real `~/credentials` (see below) so this test can never accidentally touch it.

**1. Start a local Couchbase and do initial setup.** If you don't already have one running, the quickest path is the official Docker image plus the Web UI setup wizard:

```bash
docker run -d --name couchbase -p 8091-8097:8091-8097 -p 9123:9123 -p 11207:11207 -p 11210:11210 -p 11280:11280 -p 18091-18097:18091-18097 couchbase
```

Open `http://localhost:8091`, run through the setup wizard (single-node cluster is fine), and create an admin user — the examples below assume `admin` / `admin12`. See [README.md § Installing and configuring Couchbase](../README.md#installing-and-configuring-couchbase-for-metjson2db) for the general walkthrough (bucket/scope/collection concepts, index adviser, etc.).

**2. Create a bucket, plus the scope/collection/index this test needs.** Create a bucket (e.g. `metplusdata`) via the Web UI or REST API if you don't have one, then create the `MET_tests` collection under its `_default` scope and a primary index, via the REST/query API:

```bash
curl -s -u admin:admin12 -X POST \
  http://localhost:8091/pools/default/buckets/metplusdata/scopes/_default/collections \
  -d name=MET_tests

curl -s -u admin:admin12 -X POST http://localhost:8093/query/service \
  -d 'statement=CREATE PRIMARY INDEX ON `metplusdata`.`_default`.`MET_tests`'
```

**3. Create a test-only credentials file** — do **not** reuse or overwrite your real `~/credentials`:

```bash
mkdir -p /tmp/cb-test-home
cat > /tmp/cb-test-home/credentials <<EOF
cb_host: couchbase://localhost
cb_bucket: metplusdata
cb_scope: _default
cb_collection: MET_tests
cb_user: admin
cb_password: admin12
EOF
```

**4. Run the test with `$HOME` pointed at that directory** (`testMerge_Init` reads `~/credentials` via `os.UserHomeDir()`, so overriding `$HOME` for just this command is what keeps it isolated from your real credentials):

```bash
HOME=/tmp/cb-test-home go test -tags integration ./pkg/black_box_tests/ -v
```

Add `-race` to also check for concurrency issues in the async worker pool — this is the only test in the repo that exercises real concurrent Couchbase traffic, so it's the one place `-race` here is actually meaningful:

```bash
HOME=/tmp/cb-test-home go test -race -tags integration ./pkg/black_box_tests/ -v
```

**Stop Couchbase when done:**

```bash
docker rm -f couchbase
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

## End-to-end test with real tarballs (MiniStack)

Use this workflow to verify that a real tarball of stat files is correctly streamed from S3, parsed, and converted to JSON — without needing a Couchbase database.

### Step 1 — Start MiniStack

```bash
docker run --name ministack -d -p 4566:4566 ministackorg/ministack
curl -s http://localhost:4566/_ministack/health  # should return 200
```

### Step 2 — Create a bucket and queue

```bash
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export ENDPOINT=http://localhost:4566

aws --endpoint-url $ENDPOINT s3 mb s3://met-test-bucket
aws --endpoint-url $ENDPOINT sqs create-queue --queue-name met-test-queue
# note the QueueUrl from the output
```

### Step 3 — Upload your tarball

```bash
aws --endpoint-url $ENDPOINT s3 cp your_stat_files.tar.gz \
  s3://met-test-bucket/uploads/your_stat_files.tar.gz
```

### Step 4 — Send an S3 ObjectCreated event to SQS

```bash
aws --endpoint-url $ENDPOINT sqs send-message \
  --queue-url http://localhost:4566/000000000000/met-test-queue \
  --message-body '{
    "Records": [{
      "eventName": "ObjectCreated:Put",
      "s3": {
        "bucket": {"name": "met-test-bucket"},
        "object": {"key": "uploads/your_stat_files.tar.gz", "size": 0}
      }
    }]
  }'
```

### Step 5 — Run the worker with JSON output

The `--json-output` flag switches the worker into `CREATE_JSON_DOC_ARCHIVE` mode and sets the output path prefix. No database connection is required.

```bash
SQS_QUEUE_URL=http://localhost:4566/000000000000/met-test-queue \
  go run ./cmd/sqsworker \
    --endpoint http://localhost:4566 \
    -l ./load_spec.json \
    --json-output /tmp/metjson2db_out_
```

The worker processes the tarball, writes the output, deletes the SQS message, and waits for the next message. Press `Ctrl+C` to stop it.

### Step 6 — Inspect the output

```bash
# List what was written
ls /tmp/metjson2db_out_*.json.gz
gunzip /tmp/metjson2db_out_*.json.gz
```

Then inspect with any JSON tool. The filename includes a timestamp, so use a glob to find it:

```bash
OUTPUT=$(ls /tmp/metjson2db_out_*.json | head -1)

# Summary: total doc count and a sample of keys
python3 -c "
import json
docs = json.load(open('$OUTPUT'))
print(f'Total documents: {len(docs)}')
for key in list(docs.keys())[:3]:
    print(f'  {key}')
"

# Pretty-print a single document
python3 -c "
import json
docs = json.load(open('$OUTPUT'))
key = next(iter(docs))
print(json.dumps(docs[key], indent=2))
"
```

### Stop MiniStack when done

```bash
docker rm -f ministack
```

## Docker

```bash
docker build -t sqsworker .
```

The Dockerfile builds the `sqsworker` binary and sets it as the entrypoint. To run:

```bash
docker run -e SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789/my-queue \
  sqsworker -l /app/load_spec.json
```

To build the `metjson2db` CLI binary, use `go build` directly:

```bash
go build -o build/metjson2db ./cmd/metjson2db
```
