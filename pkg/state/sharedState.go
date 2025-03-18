package state

import (
	"sync"

	"github.com/NOAA-GSL/METdatacb/pkg/types"
)

var (
	Conf                = types.ConfigJSON{}
	TroubleShoot        = types.TroubleShoot{}
	CbLineTypeColDefs   map[string]types.ColDefArray
	TotalLinesProcessed = 0
	//CbDocs              map[string]types.CbDataDocument
	CbDocs        map[string]interface{}
	CbDocMutexMap map[string]*sync.RWMutex
	CbDocsMutex   *sync.RWMutex
	DataKeyIdx    int
	Credentials   = types.Credentials{}
)

var (
	AsyncFileProcessorChannels  []chan string
	AsyncFlushToFileChannels    []chan map[string]interface{}
	AsyncFlushToDbChannels      []chan map[string]interface{}
	AsyncWaitGroupFileProcessor sync.WaitGroup
	AsyncWaitGroupFlushToFiles  sync.WaitGroup
	AsyncWaitGroupFlushToDb     sync.WaitGroup
)

var (
	LineTypeStats       map[string]types.LineTypeStat
	DocKeyCountMapMutex *sync.RWMutex
	DocKeyCountMap      map[string]types.DocKeyCounts
)

// init runs before main() is evaluated
func init() {
	CbLineTypeColDefs = make(map[string]types.ColDefArray)
	CbDocs = make(map[string]interface{})
	CbDocMutexMap = make(map[string](*sync.RWMutex))
	CbDocsMutex = &sync.RWMutex{}
	DocKeyCountMapMutex = &sync.RWMutex{}
	DocKeyCountMap = make(map[string]types.DocKeyCounts)
	LineTypeStats = make(map[string]types.LineTypeStat)
}

var StatToCbRun = types.StatToCbRun{}
