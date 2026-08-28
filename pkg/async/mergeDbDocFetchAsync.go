package async

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/dtcenter/METjson2db/pkg/state"
	"github.com/dtcenter/METjson2db/pkg/utils"
)

// init runs before main() is evaluated
func init() {
	slog.Debug("flushToDbAsync:init()")
}

func MergeDbDocFetchAsync(ctx context.Context, threadIdx int, ch chan string) {
	conn := state.DbConn
	count := 0
	errors := 0
	for {
		id, ok := <-ch
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
		if len(dbReadDoc) > 0 && len(dbReadDoc["data"].(map[string]interface{})) > 0 {
			state.CbMergeDbDocsMutex.Lock()
			state.CbMergeDbDocs[id] = dbReadDoc
			state.CbMergeDbDocsMutex.Unlock()
			count += 1
		} else {
			slog.Info(fmt.Sprintf("error, dbReadDoc:%v", dbReadDoc))
		}
	}

	state.CbMergeDbDocsMutex.RLock()
	totalMergeDocs := len(state.CbMergeDbDocs)
	state.CbMergeDbDocsMutex.RUnlock()
	slog.Info(fmt.Sprintf("MergeDbDocFetchAsync(%d) doc count:[thread:%d,total:%d], errors:%d", threadIdx, count, totalMergeDocs, errors))
	returnDoc := fmt.Sprintf("%d", count)
	ch <- returnDoc
}
