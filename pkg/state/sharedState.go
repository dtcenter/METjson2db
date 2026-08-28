package state

import (
	"sync"
	"sync/atomic"

	"github.com/dtcenter/METjson2db/pkg/types"
)

var (
	LoadSpec            = types.LoadSpec{}
	TotalLinesProcessed = 0
	CbDocs              map[string]interface{}
	CbDocsMutex         *sync.RWMutex
	CbMergeDbDocs       map[string]interface{}
	CbMergeDbDocsMutex  *sync.RWMutex
	DataKeyIdx          int
	Credentials         = types.Credentials{}
	METParserNewDocId   string
	MergeTestDocs       map[string]interface{}

	// DbConn is the single Couchbase connection established once at startup and reused for the
	// life of the process — see utils.GetDbConnection. Not reset by StateReset, same as Credentials.
	DbConn types.CbConnection
)

// DbUpsertErrors counts failed Upsert calls across this run (both the threaded and non-threaded
// paths). Unlike DbConn, this *is* reset by StateReset — it's per-run, not per-process — and
// ProcessFromProvider checks it after flushing to decide whether to return an error, so a failed
// write stops the SQS message from being deleted instead of being silently dropped.
var DbUpsertErrors atomic.Int64

var (
	AsyncFlushToDbChannels      []chan map[string]interface{}
	AsyncMergeDocFetchChannels  []chan string
	AsyncWaitGroupFlushToDb     sync.WaitGroup
	AsyncWaitGroupMergeDocFetch sync.WaitGroup
)

var (
	LineTypeStats       map[string]types.LineTypeStat
	DocKeyCountMapMutex *sync.RWMutex
	DocKeyCountMap      map[string]types.DocKeyCounts
)

// init runs before main() is evaluated
func init() {
	CbDocsMutex = &sync.RWMutex{}
	DocKeyCountMapMutex = &sync.RWMutex{}
	CbMergeDbDocsMutex = &sync.RWMutex{}
	StateReset()
}

func StateReset() {
	CbDocs = make(map[string]interface{})
	CbMergeDbDocs = make(map[string]interface{})
	DocKeyCountMap = make(map[string]types.DocKeyCounts)
	LineTypeStats = make(map[string]types.LineTypeStat)
	AsyncFlushToDbChannels = make([]chan map[string]interface{}, 0)
	AsyncMergeDocFetchChannels = make([]chan string, 0)
	DbUpsertErrors.Store(0)
}

var StatToCbRun = types.StatToCbRun{}
