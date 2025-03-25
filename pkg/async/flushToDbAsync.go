package async

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/NOAA-GSL/METdatacb/pkg/state"
	"github.com/NOAA-GSL/METdatacb/pkg/types"
	"github.com/NOAA-GSL/METdatacb/pkg/utils"
)

// init runs before main() is evaluated
func init() {
	slog.Debug("flushToDbAsync:init()")
}

func FlushToDbAsync(threadIdx int /*, conn CbConnection*/) {
	conn := utils.GetDbConnection(state.Credentials)
	count := 0
	mergeCount := 0
	errors := 0
	for {
		doc, ok := <-state.AsyncFlushToDbChannels[threadIdx]
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

		// slog.Info(fmt.Sprintf("FlushToDbAsync(), doc:%v", doc))
		id := doc["id"].(string)
		//state.CbDocMutexMap[id].Lock()
		// slog.Debug("flushToDbAsync(%d), ID:%s", threadIdx, doc.headerFields["ID"].StringVal)

		if false == state.Conf.OverWriteData && state.Conf.RunMode == "DIRECT_LOAD_TO_DB" {
			state.CbMergeDbDocsMutex.RLock()
			tmpDbDoc := state.CbMergeDbDocs[id]
			state.CbMergeDbDocsMutex.RUnlock()

			if tmpDbDoc == nil {
				slog.Info("no merge doc found for id:" + id)
			} else {
				dbDoc := tmpDbDoc.(map[string]interface{})
				// slog.Info("dbDoc:\n" + utils.DocPrettyPrint(dbDoc))
				// we need to merge
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
						json.Unmarshal(inrec, &docData)
						for dbDataKey, dbDataVal := range dbVal.(map[string]interface{}) {
							docDataVal := docData[dbDataKey]
							if docDataVal == nil {
								docData[dbDataKey] = dbDataVal
							}
						}
						doc["data"] = docData
					}
				}
				mergeCount = mergeCount + 1
			}
		}

		if doc["data"] == nil {
			slog.Debug(fmt.Sprintf("NULL document[%s]", doc["ID"]))
			errors++
			//state.CbDocMutexMap[id].Unlock()
			continue
		}

		// doc.Flushed = true

		/*
			https://docs.couchbase.com/go-sdk/current/howtos/kv-operations.html
			CAS - Compare and Swap (CAS)
		*/

		// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
		_, err := conn.Collection.Upsert(id, doc, nil)
		if err != nil {
			slog.Error(fmt.Sprintf("%v", err))
			slog.Error(fmt.Sprintf("******* Upsert error:ID:%s", id))
			// doc.Flushed = false
		} else {
			count++

			state.DocKeyCountMapMutex.Lock()
			state.DocKeyCountMap[id] = types.DocKeyCounts{HeaderLen: len(doc) - 1, DataLen: len(doc)}
			state.DocKeyCountMapMutex.Unlock()

			/*
				if state.TroubleShoot.EnableTrackContextFlushToDb {
					for i := 0; i < len(state.TroubleShoot.IdTrack.IdList); i++ {
						if id == state.TroubleShoot.IdTrack.IdList[i] || state.TroubleShoot.IdTrack.IdList[i] == "*" {
							if slices.Contains(state.TroubleShoot.IdTrack.Actions, "logJSON") {
								slog.Debug(">>>>>>>>>>>>> Tracking[logJSON] doc:\n%s\n", doc.ToJSONString())
							}
							if slices.Contains(state.TroubleShoot.IdTrack.Actions, "verifyWithDbRead") {
								dbReadDoc := utils.GetDocWithId(conn.Collection, id)
								if dbReadDoc == nil {
									slog.Debug(">>>>>>>>>>>>> Tracking[verifyWithDbRead] ID:%s, null data!!!", id)
									if state.TroubleShoot.TerminateAtFirstTrackError {
										slog.Error("Terminating due to track error ....")
									}
								} else {
									slog.Debug(">>>>>>>>>>>>> Tracking[verifyWithDbRead] ID:%s, headerFields:[cur:%d, db:%d], data:[cur:%d, db:%d]", dbReadDoc["ID"],
										len(doc.HeaderFields), len(dbReadDoc)-1, len(doc.Data), len(dbReadDoc["data"].(map[string]interface{})))
									if len(doc.HeaderFields) != (len(dbReadDoc)-1) || len(doc.Data) != len(dbReadDoc["data"].(map[string]interface{})) {
										slog.Debug("******************** >>>>>>>>>>>>> Tracking[verifyWithDbRead], data mismatch: ID:%s, headerFields:[cur:%d, db:%d], data:[cur:%d, db:%d]", dbReadDoc["ID"],
											len(doc.HeaderFields), len(dbReadDoc)-1, len(doc.Data), len(dbReadDoc["data"].(map[string]interface{})))
										if state.TroubleShoot.TerminateAtFirstTrackError {
											slog.Error("Terminating due to track error ....")
										}
									}
								}
							}

							if slices.Contains(state.TroubleShoot.IdTrack.Actions, "trackDataKeyCount") {
								slog.Debug(">>>>>>>>>>>>> Tracking[trackDataKeyCount] doc.headerFields:%d, doc.data:[prev:%d, cur:%d]",
									len(doc.HeaderFields), state.DocKeyCountMap[id].DataLen, len(doc.Data))
							}

							if slices.Contains(state.TroubleShoot.IdTrack.Actions, "checkForEmptyDoc") {
								if len(doc.HeaderFields) == 0 {
									slog.Debug("******************** >>>>>>>>>>>>> Tracking[checkForEmptyDoc] doc.headerFields:%d, doc.data:%d", len(doc.HeaderFields), len(doc.Data))
									if state.TroubleShoot.TerminateAtFirstTrackError {
										slog.Error("Terminating due to track error ....")
									}
								}
							}
						}
					}
				}
			*/
		}
		//state.CbDocMutexMap[id].Unlock()
	}
	slog.Info(fmt.Sprintf("flushToDbAsync(%d) doc count:%d, doc merge count:%d, errors:%d", threadIdx, count, mergeCount, errors))
	returnDoc := make(map[string]interface{})
	// returnDoc.InitReturn(int64(count), int64(errors))
	state.AsyncFlushToDbChannels[threadIdx] <- returnDoc
}
