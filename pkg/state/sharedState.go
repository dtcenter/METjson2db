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
	CbDocs              map[string]types.CbDataDocument
	CbDocMutexMap       map[string]*sync.RWMutex
	CbDocsMutex         *sync.RWMutex
	DataKeyIdx          int
	Credentials         = types.Credentials{}
)

var (
	AsyncFileProcessorChannels  []chan string
	AsyncFlushToFileChannels    []chan types.CbDataDocument
	AsyncFlushToDbChannels      []chan types.CbDataDocument
	AsyncWaitGroupFileProcessor sync.WaitGroup
	AsyncWaitGroupFlushToFiles  sync.WaitGroup
	AsyncWaitGroupFlushToDb     sync.WaitGroup
)

var (
	LineTypeStats       map[string]types.LineTypeStat
	DocKeyCountMapMutex *sync.RWMutex
	DocKeyCountMap      map[string]types.DocKeyCounts
)

var StatToCbRun = types.StatToCbRun{}
