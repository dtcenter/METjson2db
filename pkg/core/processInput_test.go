package core

import (
	"testing"
	"time"

	"github.com/dtcenter/METjson2db/pkg/state"
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
