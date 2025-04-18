package async

import (
	"fmt"
	"log/slog"

	"github.com/NOAA-GSL/METjson2db/pkg/state"
	"github.com/NOAA-GSL/METjson2db/pkg/utils"
)

// init runs before main() is evaluated
func init() {
	slog.Debug("flushToDbAsync:init()")
}

func MergeDbDocFetchAsync(threadIdx int /*, conn CbConnection*/) {
	conn := utils.GetDbConnection(state.Credentials)
	count := 0
	errors := 0
	for {
		id, ok := <-state.AsyncMergeDocFetchChannels[threadIdx]
		slog.Debug("MergeDbDocFetchAsync:" + id)
		if !ok {
			slog.Debug(fmt.Sprintf("\tMergeDbDocFetchAsync(%d), no documents in channel!", threadIdx))
			break
		}

		if id == "endMarker" {
			slog.Debug(fmt.Sprintf("\tMergeDbDocFetchAsync(%d), end-marker received!", threadIdx))
			break
		}

		state.CbMergeDbDocsMutex.RLock()
		if state.CbMergeDbDocs[id] != nil {
			state.CbMergeDbDocsMutex.RUnlock()
			continue
		}
		state.CbMergeDbDocsMutex.RUnlock()

		dbReadDoc := utils.GetDocWithId(conn.Collection, id)
		if dbReadDoc != nil && len(dbReadDoc) > 0 && len(dbReadDoc["data"].(map[string]interface{})) > 0 {
			state.CbMergeDbDocsMutex.Lock()
			state.CbMergeDbDocs[id] = dbReadDoc
			state.CbMergeDbDocsMutex.Unlock()
			count += 1
		} else {
			slog.Info(fmt.Sprintf("error, dbReadDoc:%v", dbReadDoc))
		}
	}

	slog.Info(fmt.Sprintf("MergeDbDocFetchAsync(%d) doc count:[thread:%d,total:%d], errors:%d", threadIdx, count, len(state.CbMergeDbDocs), errors))
	returnDoc := fmt.Sprintf("%d", count)
	state.AsyncMergeDocFetchChannels[threadIdx] <- returnDoc
}
