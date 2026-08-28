package async

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/couchbase/gocb/v2"

	"github.com/dtcenter/METjson2db/pkg/state"
	"github.com/dtcenter/METjson2db/pkg/telemetry"
	"github.com/dtcenter/METjson2db/pkg/types"
)

// init runs before main() is evaluated
func init() {
	slog.Debug("flushToDbAsync:init()")
}

// isConnectivityError reports whether err reflects a Couchbase connectivity problem (timeout,
// canceled request, service unavailable) rather than a data-level failure (bad argument, document
// too large, etc.), so DbConnectionErrors only counts what its name promises.
func isConnectivityError(err error) bool {
	return errors.Is(err, gocb.ErrTimeout) ||
		errors.Is(err, gocb.ErrRequestCanceled) ||
		errors.Is(err, gocb.ErrServiceNotAvailable)
}

func FlushToDbAsync(ctx context.Context, threadIdx int, ch chan map[string]interface{}) {
	conn := state.DbConn
	count := 0
	mergeCount := 0
	errors := 0
	for {
		doc, ok := <-ch
		if !ok {
			slog.Debug(fmt.Sprintf("\tflushToDbAsync(%d), no documents in channel!", threadIdx))
			break
		}

		if doc["endMarker"] != nil {
			slog.Debug(fmt.Sprintf("\tflushToDbAsync(%d), end-marker received!", threadIdx))
			break
		}

		if doc == nil || doc["id"] == nil {
			slog.Debug(fmt.Sprintf("\tflushToDbAsync(%d), nil doc or doc id", threadIdx))
			break
		}

		id := doc["id"].(string)

		if !state.LoadSpec.OverWriteData && state.LoadSpec.RunMode == "DIRECT_LOAD_TO_DB" {
			state.CbMergeDbDocsMutex.RLock()
			tmpDbDoc := state.CbMergeDbDocs[id]
			state.CbMergeDbDocsMutex.RUnlock()

			if tmpDbDoc == nil {
				slog.Info("no merge doc found for id:" + id)
			} else {
				dbDoc := tmpDbDoc.(map[string]interface{})
				mergeCountIncrDone := false
				for dbKey, dbVal := range dbDoc {
					if dbKey != "data" {
						// header field
						if doc[dbKey] == nil {
							doc[dbKey] = dbVal
						}
					} else {
						// data fields
						var docData map[string]interface{}
						inrec, _ := json.Marshal(doc["data"])
						err := json.Unmarshal(inrec, &docData)
						if err != nil {
							slog.Error(fmt.Sprintf("%v", err))
						}
						for dbDataKey, dbDataVal := range dbVal.(map[string]interface{}) {
							docDataVal := docData[dbDataKey]
							if docDataVal == nil {
								docData[dbDataKey] = dbDataVal
								if !mergeCountIncrDone {
									mergeCount++
									telemetry.DocumentsMerged.Add(ctx, 1)
									mergeCountIncrDone = true
								}
							}
						}
						doc["data"] = docData
					}
				}
			}
		}

		if doc["data"] == nil {
			slog.Debug(fmt.Sprintf("NULL document[%s]", doc["ID"]))
			errors++
			continue
		}

		// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
		upsertStart := time.Now()
		_, err := conn.Collection.Upsert(id, doc, nil)
		telemetry.DbUpsertDuration.Record(ctx, time.Since(upsertStart).Seconds())
		if err != nil {
			telemetry.DocumentsUpserted.Add(ctx, 1, telemetry.StatusError)
			if isConnectivityError(err) {
				telemetry.DbConnectionErrors.Add(ctx, 1)
			}
			state.DbUpsertErrors.Add(1)
			slog.Error(fmt.Sprintf("%v", err))
			slog.Error(fmt.Sprintf("******* Upsert error:ID:%s", id))
		} else {
			telemetry.DocumentsUpserted.Add(ctx, 1, telemetry.StatusSuccess)
			count++

			state.DocKeyCountMapMutex.Lock()
			state.DocKeyCountMap[id] = types.DocKeyCounts{HeaderLen: len(doc) - 1, DataLen: len(doc)}
			state.DocKeyCountMapMutex.Unlock()
		}
	}
	slog.Info(fmt.Sprintf("flushToDbAsync(%d) doc count:%d, doc merge count:%d, errors:%d", threadIdx, count, mergeCount, errors))
	returnDoc := make(map[string]interface{})
	ch <- returnDoc
}
