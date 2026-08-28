# Plan: Couchbase upsert reliability & DNS debuggability

**Status (2026-08-28):** Paused here for review. PR #77 (observability metrics) merged to `main`.
This work is [PR #79](https://github.com/dtcenter/METjson2db/pull/79), branch
`improve-upsert-debugging`, based on `main`.

- **Goal 1 (data-loss fix) — done, both parts, live-verified.** Part A (single process-lifetime
  connection) and Part B (error propagation) are implemented and committed. See
  [Goal 1](#goal-1-stop-deleting-sqs-messages-when-upserts-failed-data-loss-fix) for what's still
  open (merge-fetch-failure logging) and what wasn't verifiable by test (the redelivery trigger
  itself, since `gocb.Collection` isn't mockable).
- **Goal 2 (auto-normalize `cb_host`) — done, live-verified.**
- **Goal 3 (DNS self-check → error-context enrichment) — done, folded into Part A's work** since
  both touched `GetDbConnection`'s error handling at the same time.
- **Goal 4 (document the incident) — done.** New `docs/troubleshooting.md`, cross-linked from
  `docs/architecture.md` and `docs/observability.md`.
- **Five additional bugs were found and fixed along the way** (two data races, a scope-derivation
  bug, a run-mode gating bug, and a log copy-paste), not originally in this plan — see
  [Additional fixes found along the way](#additional-fixes-found-along-the-way). All were caught
  only because `pkg/black_box_tests` got fixed and actually run against a real Couchbase instance;
  none would have been caught by the existing unit test suite or by code review alone.
- **`pkg/black_box_tests` is fixed and usable again** (it hadn't compiled in a long time — see
  below) and now documented in `docs/dev-guide.md`, including `-race` usage, since it's the only
  test in the repo that exercises real concurrent Couchbase traffic.

Decisions made along the way:

- **Goal 1's retry tradeoff is accepted**: it's fine for a partially-processed message to go back on
  the queue on retry. Upserts must be idempotent; if they aren't, that's a bug to fix on its own
  merits, not a reason to avoid propagating real errors.
- **Goal 1 is built around a single, process-lifetime Couchbase connection** instead of one
  connection per worker per message — see [Implementation](#implementation) below. This was prompted
  by asking "is there a way to fail early, since we're just going to retry anyway?" — the answer
  turned out to be architectural, not just an early-exit check.
- **Goal 2 (auto-normalize) is done** — approved contingent on no surprising side effects (see the
  gotchas list below, all of which checked out clean or are guarded against), then implemented and
  live-verified.
- **Goal 3 (DNS self-check) is dropped as a separate mechanism.** Skipped in favor of a much smaller
  change: better error context in `GetDbConnection`'s existing error handling. Renamed accordingly.
- **Merge-fetch failure is a real, accepted data-integrity risk, not just a degraded-quality one** —
  see [Known issues & limitations](#known-issues--limitations). Still just logged generically, not
  flagged explicitly or fixed; the real fix needs a bigger redesign that's out of scope here.

## Background

A 2026-08 production incident took ~5 hours to diagnose: Couchbase upserts were failing/timing out
with no clear signal pointing at the cause. Root cause was the combination of:

- The `sqsworker` image is built `CGO_ENABLED=0` against `gcr.io/distroless/static`
  ([Dockerfile:1,11,13](../../Dockerfile)), so Go uses its pure-Go `netgo` DNS resolver instead of
  glibc's resolver.
- Kubernetes sets `ndots:5` in pod `/etc/resolv.conf` by default, so `netgo` appends the cluster's
  search domains (e.g. `.default.svc.cluster.local`, ...) to every lookup of the Couchbase Capella
  hostname before falling through to the real public DNS query.
- The cumulative latency of those failed search-domain lookups was enough to blow through gocb's KV
  operation timeouts, so individual `Upsert` calls failed/timed out.
- The fix applied in the moment was manually adding a trailing dot to `cb_host` in the credentials
  file, forcing the hostname to be treated as an absolute FQDN and skipping search-domain expansion.

Two follow-up concerns came out of this:

1. **Nothing in the code makes this failure mode easy to detect quickly.** The manual fix lives only
   in a config file and institutional memory — there's no startup signal that would have surfaced
   the problem in seconds instead of hours, and no documentation of the incident despite an earlier
   commit message claiming docs were updated (verified: they weren't — see
   [Goal 4](#goal-4-document-the-incident)).
2. **When upserts fail, the code doesn't stop the SQS message from being deleted anyway** — a
   correctness bug independent of the DNS issue, but the DNS issue is exactly the kind of failure
   mode that triggers it. This is the more serious problem: it's silent data loss.

## Goals

### Goal 1: Stop deleting SQS messages when upserts failed (data-loss fix)

**Priority: highest — this is the actual data-loss bug. Status: done (both Part A and Part B),
committed on PR #79, live-verified against a real Couchbase instance.**

Original behavior (now fixed — kept here for context on what changed and why):

- `pkg/core/processInput.go` `ProcessFromProvider` declared `dbTotalErrors` but never incremented
  it, and unconditionally returned `nil` regardless of whether any upsert failed.
- Upsert failures were only logged, never surfaced as errors, in both the threaded
  (`pkg/async/flushToDbAsync.go`) and non-threaded (`pkg/core/statToCbFlush.go`) paths.
- `pkg/utils/db-utils.go` `GetDbConnection` returned a zero-value `CbConnection{}` on
  `gocb.Connect`/`WaitUntilReady` failure with no error return at all.
- `cmd/sqsworker/main.go`'s `handleMessage`/`pollLoop` already did the right thing **given a
  non-nil error**: skip `DeleteMessage`, log "processing message failed, leaving in queue for
  retry". The plumbing above them was what was broken — all of it addressed below.

#### Implementation

**Part A — consolidate to a single, process-lifetime Couchbase connection.**

Today, `GetDbConnection(state.Credentials)` is called fresh at the top of every worker goroutine
(`FlushToDbAsync`, `MergeDbDocFetchAsync`), and `ProcessFromProvider` spawns a new batch of workers
on *every* SQS message. In a long-running `sqsworker` process, that's a fresh TLS handshake + auth
round-trip per worker per message — thousands of redundant reconnects over the process's life. It's
also not how gocb is meant to be used: a `*gocb.Cluster` (and its `Collection`/`Bucket`/`Scope`
children) is documented as safe for concurrent use from many goroutines and is meant to be created
**once** and reused for the application's lifetime — it manages its own internal connection pool and
reconnection logic. Reconnecting per-message works against that.

This also directly answers "is there a way to fail early, since we're retrying anyway?" — yes: if
connection establishment happens once, up front, before any S3/parsing work or worker spawning, a
bad connection means `run()` returns immediately, before `pollLoop` ever starts. No wasted work on a
message that was never going to succeed, and no per-worker "what if my connection failed" branch to
design around — that whole class of problem (see the channel-drain scenario worked through earlier
in this conversation) stops existing, because a worker is never spawned without an already-good
connection.

1. Add `DbConn types.CbConnection` to `pkg/state/sharedState.go` — process-lifetime state, **not**
   reset by `state.StateReset()` (same treatment as `Credentials`, which also isn't reset per-run).
2. Give `GetDbConnection` a proper `(types.CbConnection, error)` signature. It keeps its existing
   job — establish one connection — it's just called once now instead of once per worker.
3. In `cmd/sqsworker/main.go`'s `run()`, right after loading credentials (Goal 2's normalization
   still pending — see Goal 2), call `GetDbConnection` once, store the result in `state.DbConn`, and
   return the error immediately on failure — before AWS config loading, before `pollLoop`.
   `cmd/metjson2db` gets the same treatment before its run-mode dispatch, via a shared
   `core.ConnectDbIfNeeded()` helper gated on an allow-list of run modes that actually touch
   Couchbase (`DIRECT_LOAD_TO_DB`, `METADATA_UPDATE`) — `CREATE_JSON_DOC_ARCHIVE` never connects.
4. `FlushToDbAsync`, `MergeDbDocFetchAsync`, `StatToCbFlush`'s non-threaded `flushToDb` path, and
   `pkg/metadataUpdate/metadataUpdate.go` all read `state.DbConn` directly instead of calling
   `GetDbConnection` themselves. `pkg/black_box_tests/merge_test.go` can keep calling
   `GetDbConnection` directly (test fixtures establishing their own connection is fine) but needs the
   new two-return signature at each of its three call sites.
5. **What happens if Couchbase becomes unreachable *after* startup** (mid-process-life outage)? No
   special handling needed — individual `Upsert`/`Get` calls on the shared `Collection` will just
   start erroring (gocb's internal pool/retry logic handles the transport plumbing), which is exactly
   what Part B below turns into a real per-message error and a queue redelivery. This is the correct,
   resilient behavior, and it falls out of the design for free.

**Part B — propagate upsert failures as real errors** (the original core of Goal 1):

1. Add `DbUpsertErrors atomic.Int64` to `pkg/state/sharedState.go` (the codebase has no
   `sync/atomic` usage yet — shared state elsewhere uses mutexes, but a simple counter is exactly
   what `atomic.Int64` is for). Reset it (`.Store(0)`) in `state.StateReset()` — this one *is*
   per-run, unlike `DbConn`.
2. In `flushToDbAsync.go`'s and `statToCbFlush.go`'s `Upsert` error branches, increment
   `state.DbUpsertErrors` alongside the existing logging/metrics.
3. In `ProcessFromProvider`'s `DIRECT_LOAD_TO_DB` case, right after `stopFlushToDbWorkers()`, check
   `state.DbUpsertErrors.Load()` and return an error if nonzero (finally using the already-declared
   `dbTotalErrors` variable for its originally-intended purpose).
4. No changes needed in `cmd/sqsworker/main.go`'s `handleMessage`/`pollLoop` — once
   `ProcessFromProvider` returns a real error, the existing retry-safe path (skip delete, leave
   message in queue) already kicks in automatically.

**Merge-fetch failure classification**: does *not* count toward `DbUpsertErrors` — see
[Known issues & limitations](#known-issues--limitations) for why this is a deliberate, accepted
tradeoff rather than an oversight.

**Retry-idempotency note (accepted, not blocking):** after this fix, any upsert failure sends the
*whole* message back to the queue, and the tarball is fully reprocessed on retry — `main.go`'s
existing comment already documents this as safe under `overWriteData: true`. Per your answer: this
is fine, and any idempotency gap that surfaces is a bug in its own right, not a reason to avoid
propagating errors.

**Verification (what actually happened, not just planned):**

- **Startup fail-fast, verified end-to-end with real built binaries**: `sqsworker` against a bad
  `cb_host` fails after `WaitUntilReady`'s ~5s timeout with a clear, context-rich error and exits —
  critically, never logs "ready, polling for messages," confirming it fails before touching SQS at
  all. Both `sqsworker -json-output` (CREATE_JSON_DOC_ARCHIVE) and `metjson2db` in that same mode,
  same bad credentials, correctly skip the connection attempt entirely (reached "ready"/completed in
  milliseconds, zero "database"/"couchbase" mentions in output) — confirms the run-mode allow-list
  gate works in both entrypoints.
- **Concurrent shared-connection access, live-verified against a real Couchbase instance**: fixed
  `pkg/black_box_tests` (see below) and ran `TestMerge` — 4 concurrent flush workers + 2 concurrent
  merge-fetch workers, 3,164 real documents, thousands of real `Upsert`/`Get` calls against the one
  shared `state.DbConn`, `-race`-clean, correct merge results. This is the strongest evidence the
  single-connection design (Part A) is actually safe under real concurrent load, not just reasoned
  about via the Go memory model.
- **Not verified, and honestly can't be with a unit test as this codebase is structured**: forcing a
  genuine `Upsert` failure to confirm `state.DbUpsertErrors` increments and `ProcessFromProvider`
  returns an error (Part B's actual trigger). `gocb.Collection` is a concrete SDK type, not an
  interface, so nothing here is mockable — the only way to exercise this is a live Couchbase with a
  deliberately-broken write (bad doc ID, wrong collection, etc.), which wasn't done this round. If
  picking this back up, that's the one remaining gap worth closing live.

### Goal 2: Auto-normalize `cb_host` to an FQDN (bake the incident fix into code)

**Done, implemented and live-verified as designed below.** `normalizeCbHost` was added to
`pkg/core/processInput.go` and wired into `GetCredentials` exactly as planned — one call, right after
YAML unmarshal, no changes needed to `GetDbConnection`.

The manual fix (trailing dot on `cb_host`) currently lives only in a config file
(`credentials.template:1-2`, `docs/architecture.md:175`). Nobody deploying a new environment will
know to do this unless they've read this doc or hit the same 5-hour incident.

**Implementation — where to normalize matters.** The plan originally proposed doing this inside
`GetDbConnection`, but `GetDbConnection` is called fresh in every worker goroutine, which is spawned
per `ProcessFromProvider` call — i.e. potentially once per SQS message × `ThreadsDbUpload` (+
`ThreadsMergeDocFetch`) workers, for the life of the process. Normalizing there means an Info log
line firing on every single call — noisy, and the normalization work (parse, check, rebuild) repeats
for no reason since the credentials don't change at runtime. Instead: normalize once, in
`core.GetCredentials()` right after the credentials file is loaded (`pkg/core/processInput.go`),
called once at startup in both `cmd/sqsworker/main.go` and `cmd/metjson2db`. `GetDbConnection` then
just uses `cred.Cb_host` as-is — no change needed there beyond consuming an already-normalized value.

Parse with `net/url`: `url.Parse(cred.Cb_host)` → `u.Hostname()`. Append `.` only if missing and the
host isn't an IP literal (`net.ParseIP(host) == nil`). Rebuild via `net.JoinHostPort` if a port is
present (not currently used per `credentials.template`, but handled for robustness). If parsing
produces anything unexpected — parse error, empty host, a comma in the host (the `cb_host0`
multi-host field uses commas; `cb_host` itself never does today, but fail safe rather than guess) —
return the input unchanged and let `gocb.Connect` surface the real error, rather than risk silently
corrupting a connection string in a shape this function wasn't designed for.

**Gotcha review (per your ask — must not be surprising):**

- **TLS/SNI and certificate hostname verification**: a trailing dot changes the exact string used
  as the DNS name. This was the first thing to check, but it's already been validated in the field —
  the original incident's manual fix was applied to a **Capella (TLS) connection** and worked; Go's
  `crypto/tls`/`x509` verification follows RFC 6125 and normalizes a trailing dot before comparing
  against certificate SANs, same as browsers do. Not a real risk, and there's already a real-world
  data point proving it.
- **IP-literal hosts** (e.g. local MiniStack, a k8s Service ClusterIP): guarded explicitly via
  `net.ParseIP` — never dot-appended, since a dotted IP isn't a valid hostname.
- **Already-normalized credentials** (anyone who already applied the manual fix): the `HasSuffix(host,
  ".")` check makes this a no-op — no double dot, no behavior change for already-fixed environments.
- **Local dev / Docker Compose / `/etc/hosts` resolution**: an absolute FQDN (trailing dot) resolves
  identically through `/etc/hosts` and typical local resolvers — it only changes *search-domain*
  behavior, which local dev environments don't usually have configured anyway.
- **Multi-host connection strings**: not handled, deliberately — see "fail safe" above.

**Test plan — done:**

- Unit: `TestNormalizeCbHost` in `pkg/core/processInput_test.go`, table-driven — no dot → dot
  appended; already dotted → unchanged; IPv4 and IPv6 literals → unchanged; port preserved
  (`host:8091` → `host.:8091`); empty, no-scheme, and comma-separated (`cb_host0`-style) inputs →
  unchanged. All pass.
- Live: reran `pkg/black_box_tests`' `TestMerge` (`-race`, real local Couchbase) with the now-active
  normalization in the loop — `testMerge_Init` calls `core.GetCredentials`, which now normalizes
  `cb_host: couchbase://localhost` to `couchbase://localhost.` before `ConnectDbIfNeeded` ever
  connects. 3,164 documents, thousands of concurrent `Upsert`/`Get` calls across 4 flush + 2
  merge-fetch workers, `-race`-clean, correct merge results — confirms the trailing dot doesn't break
  local (non-TLS, `/etc/hosts`-resolved) connectivity, matching the local-dev gotcha above.
- **Not independently verified**: a normalized `couchbases://` (TLS/Capella) connection specifically —
  `black_box_tests` only has a local non-TLS Couchbase available in this environment. The TLS/SNI
  reasoning above (RFC 6125 trailing-dot normalization) is unchanged and was already validated by the
  original incident's manual fix working against Capella; this just automates that same fix. Still
  worth a real Capella smoke test before/soon after this ships, if one's convenient.

### Goal 3: Enrich `GetDbConnection`'s error handling (replaces the DNS self-check)

**Done — implemented as part of Goal 1's `GetDbConnection` rewrite, since both touched the same
function's error handling at the same time.** A dedicated DNS self-check was considered and
dropped — see the for/against reasoning below, kept for the record since it explains why this
smaller version was chosen instead.

The original idea was a standalone startup DNS resolution check (`net.LookupHost` under a timeout,
logged before `gocb.Connect`). Reconsidered because Goal 2 removes the cause, not just the symptom:
once `cb_host` is always an FQDN, the specific ndots/netgo failure mode from the incident can't recur
*for that reason*. A dedicated check would then mostly re-derive what `gocb.Connect`/`WaitUntilReady`
discover a few lines later anyway — two DNS-touching code paths doing overlapping work, with no
guarantee they agree (a standalone `net.LookupHost` doesn't necessarily resolve through the same path
gocb/gocbcore uses internally). Idiomatic Go leans toward good errors from the real operation over a
synthetic pre-check duplicating it, and the actual highest-leverage gap right now is that
`GetDbConnection`'s current error handling is `slog.Error(fmt.Sprintf("%v", err))` — a bare dump with
no host, no operation, no duration. Fixing that gets most of the debuggability value for a fraction
of the maintenance surface (no new timeout to tune, no second DNS-touching path to keep in sync).

**Implementation:**

- `GetDbConnection` wraps each failure with context instead of logging internally: what host/bucket
  was being connected to, which step failed (`gocb.Connect` vs. `Bucket.WaitUntilReady`), and how
  long it took before failing. E.g.
  `fmt.Errorf("connecting to Couchbase at %s (after %v): %w", connectionString, elapsed, err)`.
- Since `GetDbConnection` now returns an error (Goal 1, Part A) instead of swallowing it, it
  shouldn't also call `slog.Error` itself — that would double-log the same failure once at the source
  and again wherever the error is handled. Log once, at the call site in `run()` where the error is
  actually acted on (matching the pattern the earlier `run()` refactor already established for other
  startup failures).
- Since Part A makes this a once-at-startup call rather than once-per-worker-per-message, there's no
  concern about this context-building work or its log line being noisy/repeated.

**Verification:** no unit test was added (same mockability gap as Goal 1 Part B — nothing here is
fakeable without a real connection attempt). Verified live by running the real `sqsworker` binary
against a bad `cb_host`: the resulting error string included the host, the failing step
(`WaitUntilReady`), and elapsed time, and was logged exactly once (at the `run()` call site, not
inside `GetDbConnection` itself).

### Goal 4: Document the incident

**Done.** Added [`docs/troubleshooting.md`](../troubleshooting.md), covering: symptom (upserts
silently failing/timing out under distroless + k8s), root cause (netgo + ndots:5 search-domain
expansion), the fix (auto-normalized FQDN via Goal 2, now automatic rather than a manual step), and
the debugging entry points Goals 1 and 3 now actually provide — the
`DbConnectionErrors`/`DocumentsUpserted{status=error}` metrics, the SQS redelivery-count signal, and
the context-rich connection error log line. Also documents the merge-fetch data-loss risk from
[Known issues & limitations](#known-issues--limitations) so it's discoverable outside this plan doc,
and links the [black-box test setup](../dev-guide.md#couchbase-merge-tests-pkgblack_box_tests) so
readers can exercise the failure paths described. Cross-linked from `docs/architecture.md` (the
`GetCredentials` row in Critical Files) and `docs/observability.md` (the `db.connection_errors`
metric row) so it's discoverable from where someone would actually be looking. This was reportedly
meant to be covered by the "Update docs and add an overview on observability" commit on PR #77
(`35214e5`) but a check of that diff shows it wasn't — confirmed no mention of `netgo`, `ndots`,
`distroless`, or `trailing dot` anywhere in the repo's docs before this change.

## Additional fixes found along the way

None of these were in the original plan — all were caught while implementing and, critically, while
*live-verifying* Goal 1 against a real Couchbase instance. None would have been caught by the
existing unit test suite (it doesn't exercise real threaded DB traffic) or by code review alone; the
races in particular only became visible once `pkg/black_box_tests` was fixed and run under `-race`.
Worth internalizing as a pattern: this codebase's real DB-integration coverage was zero before this
round of work, and that gap was hiding real bugs.

- **Two pre-existing data races**, found via `-race` against the newly-fixed integration suite:
  1. `state.AsyncFlushToDbChannels`/`AsyncMergeDocFetchChannels`: the worker-spawn loop in
     `ProcessFromProvider` kept appending to (mutating) these shared slices while earlier-spawned
     workers were already running and re-reading them by index every loop iteration. Not just
     benign — if `append` reallocated mid-read, a worker could see a torn slice header. Fixed by
     passing the channel directly into each worker as a parameter instead of an index to re-resolve
     from the shared slice.
  2. `state.CbMergeDbDocs`: every access was correctly guarded by `CbMergeDbDocsMutex` except one —
     a log line in `MergeDbDocFetchAsync` read `len(state.CbMergeDbDocs)` unguarded. Fixed by taking
     the existing `RLock`.
- **Scope-derivation bug** (caught in PR #79 review, then live-verified): `GetDbConnection` built
  `conn.Collection` via `Bucket.Collection()`, which always targets the bucket's *default* scope
  regardless of `Cb_scope` — confirmed by reading the gocb source
  (`Bucket.Collection` = `DefaultScope().Collection`). Writes via `conn.Collection` and queries via
  `conn.Scope` could silently disagree on which scope they operate in. Fixed by deriving `Collection`
  from `Scope`. Live-verified both directions: with the fix, a write to a non-default
  scope/collection landed in the right place (confirmed via direct KV `Get`, not N1QL, to avoid
  index-lag false negatives); with the bug temporarily reintroduced, the identical write failed
  outright with a `KV_COLLECTION_OUTDATED` timeout rather than silently landing in the wrong
  place — since nobody duplicates a collection name across scopes in practice, this bug's real-world
  failure mode is "breaks outright the moment anyone configures a non-default scope," not a subtle
  silent misplacement.
- **`cmd/metjson2db` `METADATA_UPDATE` gating bug** (caught in PR #79 review): the run-mode check
  used the raw `-m` CLI flag instead of the effective `state.LoadSpec.RunMode`, so
  `"runMode": "METADATA_UPDATE"` in `load_spec.json` without also passing `-m METADATA_UPDATE` on
  the command line silently fell through to the normal file-processing path instead of running the
  metadata update. Fixed to check `state.LoadSpec.RunMode`.
- **`pkg/async/mergeDbDocFetchAsync.go` copy-paste bug** (caught in PR #79 review): its `init()`
  logged `"flushToDbAsync:init()"` — a leftover from before this file was split out — making startup
  logs misleading when debugging async worker initialization.
- **`pkg/black_box_tests` didn't compile.** `merge_test.go` referenced `state.Conf`, which doesn't
  exist (the field is `state.LoadSpec`) — this integration suite hadn't run in a long time. Also hit
  a pre-existing `go vet` failure (`t.Errorf` string concatenation instead of a format string) that
  blocked even `go test` from starting. Fixed, and `testMerge_Init` now populates the `LoadSpec`
  fields the pipeline needs (previously unset, so `DIRECT_LOAD_TO_DB`'s worker-spawn conditions were
  never true) and calls `core.ConnectDbIfNeeded()`, required now that connection setup moved to the
  caller (Goal 1, Part A). Documented in
  [`docs/dev-guide.md`](../dev-guide.md#couchbase-merge-tests-pkgblack_box_tests), including the
  `$HOME`-override trick to avoid ever pointing this test's `DELETE FROM` at a real `~/credentials`.

## Known issues & limitations

### Merge-fetch failure can silently destroy previously-persisted data

**Accepted for now — logged, not fixed. Real fix needs a bigger redesign, out of scope here.**

`Collection.Upsert(id, val, opts)` in gocb is a full-document create-or-replace, not a partial patch
(that's what the separate sub-document `MutateIn` API is for). The merge feature
(`overWriteData: false`) depends on `MergeDbDocFetchAsync` successfully fetching the *existing*
document so `FlushToDbAsync` can copy forward fields the incoming document doesn't have, before
calling `Upsert`.

If the merge-fetch connection fails (or the shared connection from Part A is down at the moment of
the fetch), `MergeDbDocFetchAsync` can't populate `state.CbMergeDbDocs[id]` — and the merge code
already treats "no merge doc found" as an unremarkable case, indistinguishable from a genuinely new
ID. `FlushToDbAsync` then proceeds to `Upsert` the incoming document **unmerged**. If a document
already existed at that ID with fields accumulated from earlier deliveries, this doesn't just fail to
add new data — it **overwrites and destroys** whatever was already there, because `Upsert` replaces
the whole document. This isn't fixed by a queue retry: the damage happens the moment the (successful)
`Upsert` lands, not on some later failure that would trigger redelivery.

The two real options are (a) block the `Upsert` on merge-fetch failure too, preventing the overwrite
at the cost of coupling *all* writes to merge-fetch connectivity even for IDs that don't need
merging, or (b) redesign to a safer mutation strategy (e.g. sub-document `MutateIn` so a failed merge
fetch just means "don't touch fields I don't have" instead of "replace everything"). Both are bigger
than this round of work. Decision: for now, keep merge-fetch failures **non-fatal** (logged, metriced
via `DbConnectionErrors`, not counted toward `DbUpsertErrors`/redelivery), and additionally log
explicitly when this specific scenario occurs — i.e. when an `Upsert` proceeds after a failed merge
fetch for that ID — with wording that makes the risk unambiguous (e.g. "upserting id=%s without merge
data after merge fetch failed; any previously-persisted fields not present in the incoming document
may have just been overwritten") so it's discoverable in logs rather than a silent, undiagnosable gap.

## Sequencing

Done — `add-observability-metrics` (PR #77) merged to `main` on 2026-08-28. This work continued as
its own branch, `improve-upsert-debugging`, now based on `main` directly
([PR #79](https://github.com/dtcenter/METjson2db/pull/79)). The original reasoning — build on top of
PR #77 rather than `main`, to avoid a same-file rebase and to increment real metrics immediately —
held and is why the branch was stacked that way before #77 merged.

**Where things stand for whoever picks this up next:**

- PR #79 has several commits already reviewed and pushed (Goal 1 Parts A & B, Goal 2, Goal 3, the
  five additional bugs, and the `pkg/black_box_tests` fix + docs). Check the PR itself for exact
  commit boundaries and any review comments newer than this doc.
- The local Couchbase instance used for live verification is ephemeral (Docker) — if it's been torn
  down, the `MET_tests` collection/primary index under `_default` scope needs recreating before
  `pkg/black_box_tests` will pass again; see `docs/dev-guide.md` for the exact commands.
- All four goals are now done. What's left is the smaller open items tracked in
  [Still open](#open-questions) below — none of them blocking.

## Open questions

All resolved as of this update:

- ~~Does a nil-receiver `conn.Collection.Upsert(...)` panic or error?~~ Moot by design —
  `GetDbConnection`'s `error` return means callers never attempt an upsert on an invalid connection.
- ~~Does `MergeDbDocFetchAsync` failure count toward `DbUpsertErrors`?~~ No — see
  [Known issues & limitations](#known-issues--limitations). Logged as an accepted risk instead.
- ~~Proceed with the DNS self-check, skip it, or shrink it?~~ Dropped; replaced with the smaller
  error-context improvement in Goal 3.
- ~~`cmd/metjson2db`'s exact startup structure~~ Read and updated — `core.ConnectDbIfNeeded()` is
  called there too, gated the same way as `cmd/sqsworker`.
- ~~Does a fake/mock `gocb.Collection` interface exist for injecting upsert failures?~~ No, and it
  can't be added cheaply — `gocb.Collection` is a concrete SDK type. This is a real, permanent gap:
  Goal 1 Part B's actual failure-triggers-redelivery behavior can only ever be verified live against
  a real Couchbase instance, never by a fast unit test. Worth remembering next time this area
  changes.

Still open:

- Goal 1's one remaining gap: forcing a genuine `Upsert` failure live to confirm the redelivery
  trigger itself (see [Goal 1's verification notes](#goal-1-stop-deleting-sqs-messages-when-upserts-failed-data-loss-fix)).
- The merge-fetch-failure explicit-logging enhancement (see
  [Known issues & limitations](#known-issues--limitations)) — designed, not implemented.
- Goal 2's TLS/Capella normalization isn't independently live-verified in this environment (only
  local non-TLS Couchbase was available) — see [Goal 2's test plan](#goal-2-auto-normalize-cb_host-to-an-fqdn-bake-the-incident-fix-into-code).
