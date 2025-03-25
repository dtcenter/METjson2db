package state

import (
	"sync"

	"github.com/NOAA-GSL/METdatacb/pkg/types"
)

var (
	Conf         = types.ConfigJSON{}
	TroubleShoot = types.TroubleShoot{}
	// CbLineTypeColDefs   map[string]types.ColDefArray
	TotalLinesProcessed = 0
	//CbDocs              map[string]types.CbDataDocument
	CbDocs map[string]interface{}
	// CbDocMutexMap      map[string]*sync.RWMutex
	CbDocsMutex        *sync.RWMutex
	CbMergeDbDocs      map[string]interface{}
	CbMergeDbDocsMutex *sync.RWMutex
	DataKeyIdx         int
	Credentials        = types.Credentials{}
	METParserNewDocId  string
	MergeTestDocs      map[string]interface{}
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

	// CbDocMutexMap = make(map[string](*sync.RWMutex))
	CbDocsMutex = &sync.RWMutex{}
	DocKeyCountMapMutex = &sync.RWMutex{}
	CbMergeDbDocsMutex = &sync.RWMutex{}
	StateReset()
}

func StateReset() {
	// CbLineTypeColDefs = make(map[string]types.ColDefArray)
	CbDocs = make(map[string]interface{})
	CbMergeDbDocs = make(map[string]interface{})
	DocKeyCountMap = make(map[string]types.DocKeyCounts)
	LineTypeStats = make(map[string]types.LineTypeStat)
	AsyncFlushToDbChannels = make([]chan map[string]interface{}, 0)
	AsyncMergeDocFetchChannels = make([]chan string, 0)
}

var StatToCbRun = types.StatToCbRun{}
