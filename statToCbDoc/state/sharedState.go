package state

import (
	"sync"

	"github.com/NOAA-GSL/METdatacb/statToCbDoc/pkg/core"
	"github.com/NOAA-GSL/METdatacb/statToCbDoc/pkg/types"
)

var conf = types.ConfigJSON{}
var troubleShoot = types.TroubleShoot{}
var cbLineTypeColDefs map[string]types.ColDefArray
var totalLinesProcessed = 0
var cbDocs map[string]core.CbDataDocument
var cbDocsMutex *sync.RWMutex
var dataKeyIdx int
var credentials = types.Credentials{}

var asyncFileProcessorChannels []chan string
var asyncFlushToFileChannels []chan core.CbDataDocument
var asyncFlushToDbChannels []chan core.CbDataDocument
var asyncWaitGroupFileProcessor sync.WaitGroup
var asyncWaitGroupFlushToFiles sync.WaitGroup
var asyncWaitGroupFlushToDb sync.WaitGroup

var lineTypeStats map[string]types.LineTypeStat
var docKeyCountMapMutex *sync.RWMutex
var docKeyCountMap map[string]types.DocKeyCounts
