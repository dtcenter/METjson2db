package state

import (
	"sync"

	"github.com/NOAA-GSL/METdatacb/statToCbDoc/pkg/types"
)

var Conf = types.ConfigJSON{}
var TroubleShoot = types.TroubleShoot{}
var CbLineTypeColDefs map[string]types.ColDefArray
var TotalLinesProcessed = 0
var CbDocs map[string]types.CbDataDocument
var CbDocsMutex *sync.RWMutex
var DataKeyIdx int
var Credentials = types.Credentials{}

var AsyncFileProcessorChannels []chan string
var AsyncFlushToFileChannels []chan types.CbDataDocument
var AsyncFlushToDbChannels []chan types.CbDataDocument
var AsyncWaitGroupFileProcessor sync.WaitGroup
var AsyncWaitGroupFlushToFiles sync.WaitGroup
var AsyncWaitGroupFlushToDb sync.WaitGroup

var LineTypeStats map[string]types.LineTypeStat
var DocKeyCountMapMutex *sync.RWMutex
var DocKeyCountMap map[string]types.DocKeyCounts

var StatToCbRun = types.StatToCbRun{}
