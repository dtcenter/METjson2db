package state

import (
	"sync"

	"github.com/NOAA-GSL/METjson2db/pkg/types"
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
)

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
}

var StatToCbRun = types.StatToCbRun{}
