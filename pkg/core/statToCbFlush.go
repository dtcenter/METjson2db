package core

import (
	"fmt"
	"log/slog"

	// "github.com/couchbase/gocb/v2"

	"github.com/NOAA-GSL/METdatacb/pkg/state"
	"github.com/NOAA-GSL/METdatacb/pkg/types"
	"github.com/NOAA-GSL/METdatacb/pkg/utils"
)

// init runs before main() is evaluated
func init() {
	slog.Debug("StatToCbFlush:init()")
}

func StatToCbFlush(flushFinal bool) {
	slog.Info(fmt.Sprintf("statToCbFlush(flushFinal:%t, docs:%d, totalLinesProcessed:%d, FlushToDbDataSectionMaxCount:%d)",
		flushFinal, len(state.CbDocs),
		state.TotalLinesProcessed, state.Conf.FlushToDbDataSectionMaxCount))

	/*
		See spec in readme, section:
		# Output location, configuration and logic
	*/
	flushCount := 0
	if state.Conf.RunNonThreaded {
		for id, _ := range state.CbDocs {
			slog.Info("id:" + id)
			//if flushFinal || int64(dataLen) >= int64(state.Conf.FlushToDbDataSectionMaxCount) {
			if state.Conf.WriteJSONsToFile {
				flushToFiles(id)
			}
			if state.Conf.UploadToDb {
				conn := utils.GetDbConnection(state.Credentials)
				flushToDb(conn, id)
			}
			//}
		}
	} else {
		// distribute docs to channels, round-robin, for async processing
		idxDb := 0
		// for id, doc
		for _, doc := range state.CbDocs {
			if state.Conf.UploadToDb {
				state.AsyncFlushToDbChannels[idxDb] <- doc.(map[string]interface{})
				idxDb++
				if idxDb >= int(state.Conf.ThreadsDbUpload) {
					idxDb = 0
				}
			}
		}
	}
	slog.Debug("\tflushCount:", slog.Any("flushCount", flushCount))
}

func flushToFiles(id string) {
	// slog.Debug("flushToFiles(%s)", id)

	/*
		doc := state.CbDocs[id]
		// slog.Debug("data keys:%v", maps.Keys(doc.data))


		docStr := []byte(doc.ToJSONString())
		fileName := state.Conf.OutputFolder + "/" + id + ".json"
		err := os.WriteFile(fileName, docStr, 0o644)
		if err != nil {
			slog.Debug("Error writing output:%s", fileName)
		}
	*/
}

func flushToDb(conn types.CbConnection, id string) {
	slog.Info("flushToDb(" + id + ")")

	doc := state.CbDocs[id].(map[string]interface{})
	slog.Debug(fmt.Sprintf("%v", doc))

	/*
		TODO:
		1. Merge if conf[overWriteData] == false otherwise ovewrite doc
		2. Make flushToDb threaded and async
	*/

	var anyJson = doc

	if anyJson["data"] == nil || len(anyJson) == 0 {
		// slog.Debug("NULL document[%s]", id)
		return
	}

	if state.Conf.OverWriteData {
		// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
		_, err := conn.Collection.Upsert(id, anyJson, nil)
		if err != nil {
			slog.Error(err.Error())
		}
	} else {
		dbReadDoc := utils.GetDocWithId(conn.Collection, id)
		if dbReadDoc == nil || dbReadDoc["data"] == nil || len(dbReadDoc["data"].(map[string]interface{})) == 0 {
			// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
			_, err := conn.Collection.Upsert(doc["ID"].(string), anyJson, nil)
			if err != nil {
				slog.Error(fmt.Sprintf("%v", err))
			}
		} else {
			/*
				doc.Merge(dbReadDoc)
				// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
				_, err = conn.Collection.Upsert(doc.HeaderFields["ID"].StringVal, anyJson, nil)
				if err != nil {
					slog.Error(fmt.Sprintf("%v", err))
				}
			*/
		}
	}

	/*
		if state.TroubleShoot.EnableTrackContextFlushToDb {
			for i := 0; i < len(state.TroubleShoot.IdTrack.IdList); i++ {
				if id == state.TroubleShoot.IdTrack.IdList[i] || state.TroubleShoot.IdTrack.IdList[i] == "*" {
					if slices.Contains(state.TroubleShoot.IdTrack.Actions, "logJSON") {
						slog.Debug(">>>>>>>>>>>>> Tracking[logJSON] doc:\n%s\n", doc.ToJSONString())
					}
					if slices.Contains(state.TroubleShoot.IdTrack.Actions, "verifyWithDbRead") {
						dbReadDoc := utils.GetDocWithId(conn.Collection, id)
						trackError := false
						if dbReadDoc == nil || dbReadDoc["data"] == nil || len(dbReadDoc["data"].(map[string]interface{})) == 0 {
							slog.Debug(">>>>>>>>>>>>> Tracking[verifyWithDbRead] ID:%s, null data!!!", id)
							trackError = true
						} else {
							slog.Debug(">>>>>>>>>>>>> Tracking[verifyWithDbRead] ID:%s, headerFields:[cur:%d, db:%d], data:[cur:%d, db:%d]", dbReadDoc["ID"],
								len(doc.HeaderFields), len(dbReadDoc)-1, len(doc.Data), len(dbReadDoc["data"].(map[string]interface{})))
							if len(doc.HeaderFields) != (len(dbReadDoc)-1) || len(doc.Data) != len(dbReadDoc["data"].(map[string]interface{})) {
								slog.Debug("******************** >>>>>>>>>>>>> Tracking[verifyWithDbRead], data mismatch: ID:%s, headerFields:[cur:%d, db:%d], data:[cur:%d, db:%d]", dbReadDoc["ID"],
									len(doc.HeaderFields), len(dbReadDoc)-1, len(doc.Data), len(dbReadDoc["data"].(map[string]interface{})))
								trackError = true
							}
						}
						if trackError && state.TroubleShoot.TerminateAtFirstTrackError {
							slog.Error("Terminating due to track error ....")
						}
					}

					if slices.Contains(state.TroubleShoot.IdTrack.Actions, "checkForEmptyDocsInDb") {
						sqlStr := "SELECT META(d).id FROM metdata._default.MET_default AS d WHERE d IS null"
						// slog.Debug(">>>>>>>>>>>>> Tracking[verifyWithDbRead], SQL:\n%s", sqlStr)
						result := utils.QueryWithSQLStringMAP(conn.Scope, sqlStr)
						if len(result) > 0 {
							slog.Debug("******************** >>>>>>>>>>>>> Tracking[checkForEmptyDocsInDb] empty docs[%d] found ion DB!!!", len(result))
							slog.Error("Terminating due to track error ....")
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
