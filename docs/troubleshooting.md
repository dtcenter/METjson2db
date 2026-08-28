# Troubleshooting

## Couchbase upserts failing/timing out under Kubernetes

**Symptom:** `sqsworker` running in Kubernetes intermittently fails or times out on Couchbase
`Upsert`/`Get` calls, with no clear signal pointing at the cause. This isn't a load or capacity
problem — it can happen against an otherwise healthy Couchbase cluster.

**Root cause:** a combination of three things, all necessary together:

1. `sqsworker`'s image is built `CGO_ENABLED=0` against `gcr.io/distroless/static`
   ([Dockerfile](../Dockerfile)), so Go uses its pure-Go `netgo` DNS resolver instead of glibc's
   resolver.
2. Kubernetes sets `ndots:5` in every pod's `/etc/resolv.conf` by default. With `netgo`, a
   not-fully-qualified hostname (no trailing dot) gets the cluster's search domains (e.g.
   `.default.svc.cluster.local`, ...) appended and tried, one at a time, *before* the real public DNS
   query is attempted.
3. Each of those failed search-domain lookups costs real latency. Enough of them in sequence is
   enough to blow through gocb's KV operation timeouts, so individual `Upsert` calls fail or time
   out — even though the actual target host resolves fine once the search-domain attempts are
   exhausted.

This produces confusing symptoms: the Couchbase cluster looks healthy, credentials are correct, and
the hostname resolves fine from a plain shell (`nslookup`/`dig` don't hit the same `ndots` behavior
`netgo` does) — the failure only shows up from inside the actual Go process, under the actual
distroless+k8s combination, making it easy to lose hours chasing the wrong layer.

**Fix (now automatic):** `core.GetCredentials` calls `normalizeCbHost` on `cb_host` at startup,
appending a trailing dot to the hostname unless it's already an absolute FQDN or an IP literal. An
absolute FQDN skips search-domain expansion entirely — `netgo` goes straight to the real query. This
used to be a manual step (editing `cb_host` in the credentials file to add a trailing dot); it's now
baked into the code, so a new environment doesn't need to know this incident happened to avoid
repeating it. See
[`docs/plan/couchbase-upsert-reliability.md`](plan/couchbase-upsert-reliability.md#goal-2-auto-normalize-cb_host-to-an-fqdn-bake-the-incident-fix-into-code)
for the full design and gotcha review (TLS/SNI, IP literals, local dev resolution) behind that
change.

**If this still surfaces** (e.g. against a `cb_host0` multi-host value, which `normalizeCbHost`
deliberately leaves untouched — see the plan doc), here's what's now available to diagnose it much
faster than the original 5-hour incident:

- **`GetDbConnection`'s error is context-rich**: it names the host, which step failed
  (`gocb.Connect` vs. `Bucket.WaitUntilReady`), and how long it took before failing — logged once, at
  startup, in `run()`. A slow `WaitUntilReady` failure with elapsed time close to a DNS/search-domain
  latency budget is a strong hint this is happening again.
- **`metjson2db.db.connection_errors`** (OTel counter) increments specifically for connectivity
  failures (timeout, canceled, service-unavailable), distinct from data-level upsert errors — see
  [docs/observability.md](observability.md).
- **`metjson2db.documents.upserted{status=error}`** and **`metjson2db.db.upsert.duration`** show
  whether failures are widespread (connectivity) or occasional (individual bad documents).
- **SQS message redelivery**: since the data-loss fix (see the plan doc's Goal 1), a message whose
  upserts failed is left on the queue instead of being deleted — a spike in redelivery count for the
  same message is itself a signal that upserts are failing, without needing to correlate logs first.

To reproduce or exercise any of this locally, see
[docs/dev-guide.md § Couchbase merge tests](dev-guide.md#couchbase-merge-tests-pkgblack_box_tests) —
it's the only test in the repo that exercises real Couchbase connection/upsert traffic.

## Merge-fetch failure can silently overwrite previously-persisted data

**Status:** accepted, known risk — logged, not fixed. See
[docs/plan/couchbase-upsert-reliability.md § Known issues & limitations](plan/couchbase-upsert-reliability.md#known-issues--limitations)
for the full writeup, options considered, and why this is deliberately out of scope for now.

**Symptom:** a document that previously had fields from earlier deliveries appears to have lost
them — but there's no error, no failed upsert, and no SQS redelivery, because the write that caused
it *succeeded*.

**Root cause, briefly:** `Collection.Upsert` in gocb is a full-document create-or-replace, not a
partial patch. The merge feature (`overWriteData: false`) depends on `MergeDbDocFetchAsync`
successfully fetching the existing document so `FlushToDbAsync` can copy forward fields the incoming
document doesn't have, before calling `Upsert`. If that fetch fails — e.g. the shared Couchbase
connection is degraded at that moment — the merge code treats "no merge doc found" the same as "this
is a genuinely new ID," and `Upsert`s the incoming document **unmerged**. If a document already
existed at that ID, this overwrites and destroys whatever fields it had that the incoming document
doesn't. The damage happens at the moment of the (successful) `Upsert`, not on some later failure, so
a queue retry doesn't undo it.

**What to look for:** merge-fetch failures are logged and counted via `DbConnectionErrors`, but not
yet flagged with wording that calls out this specific risk at the point it happens (that enhancement
is still open — see the plan doc). If you suspect this has occurred, correlate `MergeDbDocFetchAsync`
error logs with document IDs that later lost fields.
