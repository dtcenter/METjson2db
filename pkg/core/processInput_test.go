package core

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dtcenter/METjson2db/pkg/state"
	"github.com/dtcenter/METjson2db/pkg/types"
)

// TestStopWorkers_UnblocksAndJoins is a regression test for the goroutine/WaitGroup leak that
// occurred when ProcessFromProvider returned early (on a provider error) without ever sending the
// end marker to already-spawned async workers. A leaked worker blocks forever on its channel
// receive, and its un-Done() WaitGroup count then deadlocks every subsequent run's Wait() call.
// It reproduces the same worker shape ProcessFromProvider spawns — block on the channel, exit on
// the end marker — without needing a real Couchbase connection, since utils.GetDbConnection isn't
// injectable here.
func TestStopWorkers_UnblocksAndJoins(t *testing.T) {
	state.StateReset()
	state.LoadSpec.ChannelBufferSizeNumberOfDocs = 4

	spawnFlushWorker := func() {
		ch := make(chan map[string]interface{}, state.LoadSpec.ChannelBufferSizeNumberOfDocs)
		state.AsyncFlushToDbChannels = append(state.AsyncFlushToDbChannels, ch)
		state.AsyncWaitGroupFlushToDb.Add(1)
		go func() {
			defer state.AsyncWaitGroupFlushToDb.Done()
			for doc := range ch {
				if doc["endMarker"] != nil {
					return
				}
			}
		}()
	}
	spawnMergeWorker := func() {
		ch := make(chan string, state.LoadSpec.ChannelBufferSizeNumberOfDocs)
		state.AsyncMergeDocFetchChannels = append(state.AsyncMergeDocFetchChannels, ch)
		state.AsyncWaitGroupMergeDocFetch.Add(1)
		go func() {
			defer state.AsyncWaitGroupMergeDocFetch.Done()
			for id := range ch {
				if id == "endMarker" {
					return
				}
			}
		}()
	}

	for i := 0; i < 3; i++ {
		spawnFlushWorker()
	}
	for i := 0; i < 2; i++ {
		spawnMergeWorker()
	}

	done := make(chan struct{})
	go func() {
		stopMergeDocFetchWorkers()
		stopFlushToDbWorkers()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stopMergeDocFetchWorkers/stopFlushToDbWorkers did not return — workers are stuck, and their un-Done() WaitGroup count would deadlock the next run")
	}
}

// TestStopWorkers_SafeWhenNoneStarted confirms the helpers are no-ops when no workers were ever
// spawned (e.g. RunNonThreaded, or OverWriteData skipping merge workers) — ProcessFromProvider's
// error path relies on this to call them unconditionally rather than duplicating the spawn-site
// run-mode checks.
func TestStopWorkers_SafeWhenNoneStarted(t *testing.T) {
	state.StateReset()

	done := make(chan struct{})
	go func() {
		stopMergeDocFetchWorkers()
		stopFlushToDbWorkers()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stopMergeDocFetchWorkers/stopFlushToDbWorkers hung with no workers started")
	}
}

// TestNormalizeCbHost covers the cb_host FQDN normalization from
// docs/plan/couchbase-upsert-reliability.md's Goal 2: append a trailing dot to skip Kubernetes's
// ndots:5 search-domain expansion, but leave anything not shaped like a plain hostname untouched.
func TestNormalizeCbHost(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"no dot gets appended", "couchbase://adb-cb1.gsd.esrl.noaa.gov", "couchbase://adb-cb1.gsd.esrl.noaa.gov."},
		{"already dotted is unchanged", "couchbase://adb-cb1.gsd.esrl.noaa.gov.", "couchbase://adb-cb1.gsd.esrl.noaa.gov."},
		{"tls scheme", "couchbases://adb-cb1.gsd.esrl.noaa.gov", "couchbases://adb-cb1.gsd.esrl.noaa.gov."},
		{"ipv4 literal is unchanged", "couchbase://10.0.0.5", "couchbase://10.0.0.5"},
		{"ipv6 literal is unchanged", "couchbase://[::1]", "couchbase://[::1]"},
		{"port is preserved", "couchbase://adb-cb1.gsd.esrl.noaa.gov:8091", "couchbase://adb-cb1.gsd.esrl.noaa.gov.:8091"},
		{"empty is unchanged", "", ""},
		{"no scheme (no parseable host) is unchanged", "adb-cb2,adb-cb3,adb-cb4", "adb-cb2,adb-cb3,adb-cb4"},
		{"comma-separated multi-host is unchanged", "couchbase://adb-cb2,adb-cb3,adb-cb4", "couchbase://adb-cb2,adb-cb3,adb-cb4"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeCbHost(tc.in)
			if got != tc.want {
				t.Errorf("normalizeCbHost(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestRunModesNeedingDb pins down the allow-list ConnectDbIfNeeded gates on — a regression guard so
// a future run mode doesn't silently start (or stop) requiring a Couchbase connection unnoticed.
func TestRunModesNeedingDb(t *testing.T) {
	cases := []struct {
		name string
		mode string
		want bool
	}{
		{"DIRECT_LOAD_TO_DB needs a connection", "DIRECT_LOAD_TO_DB", true},
		{"METADATA_UPDATE needs a connection", "METADATA_UPDATE", true},
		{"CREATE_JSON_DOC_ARCHIVE does not", "CREATE_JSON_DOC_ARCHIVE", false},
		{"empty run mode does not", "", false},
		{"unrecognized run mode does not", "SOME_FUTURE_MODE", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := runModesNeedingDb[tc.mode]; got != tc.want {
				t.Errorf("runModesNeedingDb[%q] = %v, want %v", tc.mode, got, tc.want)
			}
		})
	}
}

// TestConnectDbIfNeeded_SkipsModesThatDontNeedDb confirms ConnectDbIfNeeded takes the early-return
// branch for a run mode outside runModesNeedingDb — it must not attempt a connection (which would
// otherwise block for GetDbConnection's ~5s WaitUntilReady timeout against no real Couchbase here)
// and must not populate state.DbConn.
func TestConnectDbIfNeeded_SkipsModesThatDontNeedDb(t *testing.T) {
	origRunMode := state.LoadSpec.RunMode
	origDbConn := state.DbConn
	defer func() {
		state.LoadSpec.RunMode = origRunMode
		state.DbConn = origDbConn
	}()

	state.LoadSpec.RunMode = "CREATE_JSON_DOC_ARCHIVE"
	state.DbConn = types.CbConnection{}

	if err := ConnectDbIfNeeded(); err != nil {
		t.Fatalf("ConnectDbIfNeeded() for a run mode that doesn't need a DB returned an error: %v", err)
	}
	if state.DbConn.Cluster != nil {
		t.Error("ConnectDbIfNeeded() populated state.DbConn for a run mode not in runModesNeedingDb — it must have attempted a connection")
	}
}

// TestProcessFromProvider_RequiresDbConnForDirectLoad is a regression test for the precondition
// check added to ProcessFromProvider: DIRECT_LOAD_TO_DB with an uninitialized state.DbConn (i.e.
// ConnectDbIfNeeded was never called) must fail fast with a clear error, not panic deep inside an
// async worker's Upsert call. The check runs before the provider is ever touched, so this is safe to
// exercise with a nil provider.
func TestProcessFromProvider_RequiresDbConnForDirectLoad(t *testing.T) {
	origRunMode := state.LoadSpec.RunMode
	origDbConn := state.DbConn
	defer func() {
		state.LoadSpec.RunMode = origRunMode
		state.DbConn = origDbConn
	}()

	state.LoadSpec.RunMode = "DIRECT_LOAD_TO_DB"
	state.DbConn = types.CbConnection{}

	err := ProcessFromProvider(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("expected an error when state.DbConn is uninitialized for DIRECT_LOAD_TO_DB, got nil")
	}
	if !strings.Contains(err.Error(), "ConnectDbIfNeeded") {
		t.Errorf("error should point at ConnectDbIfNeeded as the fix, got: %v", err)
	}
}
