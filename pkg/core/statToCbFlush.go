package core

import (
	"log"

	// "github.com/couchbase/gocb/v2"

	"github.com/NOAA-GSL/METdatacb/pkg/state"
	"github.com/NOAA-GSL/METdatacb/pkg/types"
	"github.com/NOAA-GSL/METdatacb/pkg/utils"
)

// init runs before main() is evaluated
func init() {
	log.Println("StatToCbFlush:init()")
}

func StatToCbFlush(flushFinal bool) {
	log.Printf("statToCbFlush(flushFinal:%t, docs:%d, totalLinesProcessed:%d, FlushToDbDataSectionMaxCount:%d)",
		flushFinal, len(state.CbDocs),
		state.TotalLinesProcessed, state.Conf.FlushToDbDataSectionMaxCount)

	/*
		See spec in readme, section:
		# Output location, configuration and logic
	*/
	flushCount := 0
	if state.Conf.RunNonThreaded {
		for id, _ := range state.CbDocs {
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
		/*
			// distribute docs to channels, round-robin, for async processing
			idxFiles := 0
			idxDb := 0
			// for id, doc
			for _, doc := range state.CbDocs {
				doc.Mutex.RLock()
				state.DocKeyCountMapMutex.RLock()
				headerLen := len(doc.HeaderFields)
				dataLen := len(maps.Keys(doc.Data))
				if state.Conf.UpdateOnlyOnDocKeyCountChange && headerLen == state.DocKeyCountMap[doc.HeaderFields["ID"].StringVal].HeaderLen &&
					dataLen == state.DocKeyCountMap[doc.HeaderFields["ID"].StringVal].DataLen {
					doc.Mutex.RUnlock()
					state.DocKeyCountMapMutex.RUnlock()
					continue
				}
				flushed := doc.Flushed
				doc.Mutex.RUnlock()
				state.DocKeyCountMapMutex.RUnlock()
				if flushFinal || (int64(dataLen) >= state.Conf.FlushToDbDataSectionMaxCount && !flushed) {
					flushCount++
					if state.Conf.WriteJSONsToFile {
						state.AsyncFlushToFileChannels[idxFiles] <- doc
						idxFiles++
						if idxFiles >= int(state.Conf.ThreadsWriteToDisk) {
							idxFiles = 0
						}
					}
					if state.Conf.UploadToDb {
						state.AsyncFlushToDbChannels[idxDb] <- doc
						idxDb++
						if idxDb >= int(state.Conf.ThreadsDbUpload) {
							idxDb = 0
						}
					}
				}
			}
		*/
	}
	log.Printf("\tflushCount:%d", flushCount)
}

func flushToFiles(id string) {
	// log.Printf("flushToFiles(%s)", id)

	/*
		doc := state.CbDocs[id]
		// log.Printf("data keys:%v", maps.Keys(doc.data))


		docStr := []byte(doc.ToJSONString())
		fileName := state.Conf.OutputFolder + "/" + id + ".json"
		err := os.WriteFile(fileName, docStr, 0o644)
		if err != nil {
			log.Printf("Error writing output:%s", fileName)
		}
	*/
}

func flushToDb(conn types.CbConnection, id string) {
	// log.Printf("flushToDb(%s)", id)

	doc := state.CbDocs[id]
	// log.Printf("data keys:%v", maps.Keys(doc.data))

	/*
		TODO:
		1. Merge if conf[overWriteData] == false otherwise ovewrite doc
		2. Make flushToDb threaded and async
	*/

	var anyJson = doc

	if anyJson["data"] == nil || len(anyJson) == 0 {
		// log.Printf("NULL document[%s]", id)
		return
	}

	if state.Conf.OverWriteData {
		// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
		_, err := conn.Collection.Upsert(doc["ID"].(string), anyJson, nil)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		dbReadDoc := utils.GetDocWithId(conn.Collection, id)
		if dbReadDoc == nil || dbReadDoc["data"] == nil || len(dbReadDoc["data"].(map[string]interface{})) == 0 {
			// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
			_, err := conn.Collection.Upsert(doc["ID"].(string), anyJson, nil)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			/*
				doc.Merge(dbReadDoc)
				// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
				_, err = conn.Collection.Upsert(doc.HeaderFields["ID"].StringVal, anyJson, nil)
				if err != nil {
					log.Fatal(err)
				}
			*/
		}
	}

	/*
		if state.TroubleShoot.EnableTrackContextFlushToDb {
			for i := 0; i < len(state.TroubleShoot.IdTrack.IdList); i++ {
				if id == state.TroubleShoot.IdTrack.IdList[i] || state.TroubleShoot.IdTrack.IdList[i] == "*" {
					if slices.Contains(state.TroubleShoot.IdTrack.Actions, "logJSON") {
						log.Printf(">>>>>>>>>>>>> Tracking[logJSON] doc:\n%s\n", doc.ToJSONString())
					}
					if slices.Contains(state.TroubleShoot.IdTrack.Actions, "verifyWithDbRead") {
						dbReadDoc := utils.GetDocWithId(conn.Collection, id)
						trackError := false
						if dbReadDoc == nil || dbReadDoc["data"] == nil || len(dbReadDoc["data"].(map[string]interface{})) == 0 {
							log.Printf(">>>>>>>>>>>>> Tracking[verifyWithDbRead] ID:%s, null data!!!", id)
							trackError = true
						} else {
							log.Printf(">>>>>>>>>>>>> Tracking[verifyWithDbRead] ID:%s, headerFields:[cur:%d, db:%d], data:[cur:%d, db:%d]", dbReadDoc["ID"],
								len(doc.HeaderFields), len(dbReadDoc)-1, len(doc.Data), len(dbReadDoc["data"].(map[string]interface{})))
							if len(doc.HeaderFields) != (len(dbReadDoc)-1) || len(doc.Data) != len(dbReadDoc["data"].(map[string]interface{})) {
								log.Printf("******************** >>>>>>>>>>>>> Tracking[verifyWithDbRead], data mismatch: ID:%s, headerFields:[cur:%d, db:%d], data:[cur:%d, db:%d]", dbReadDoc["ID"],
									len(doc.HeaderFields), len(dbReadDoc)-1, len(doc.Data), len(dbReadDoc["data"].(map[string]interface{})))
								trackError = true
							}
						}
						if trackError && state.TroubleShoot.TerminateAtFirstTrackError {
							log.Fatal("Terminating due to track error ....")
						}
					}

					if slices.Contains(state.TroubleShoot.IdTrack.Actions, "checkForEmptyDocsInDb") {
						sqlStr := "SELECT META(d).id FROM metdata._default.MET_default AS d WHERE d IS null"
						// log.Printf(">>>>>>>>>>>>> Tracking[verifyWithDbRead], SQL:\n%s", sqlStr)
						result := utils.QueryWithSQLStringMAP(conn.Scope, sqlStr)
						if len(result) > 0 {
							log.Printf("******************** >>>>>>>>>>>>> Tracking[checkForEmptyDocsInDb] empty docs[%d] found ion DB!!!", len(result))
							log.Fatal("Terminating due to track error ....")
						}
					}

					if slices.Contains(state.TroubleShoot.IdTrack.Actions, "trackDataKeyCount") {
						log.Printf(">>>>>>>>>>>>> Tracking[trackDataKeyCount] doc.headerFields:%d, doc.data:[prev:%d, cur:%d]",
							len(doc.HeaderFields), state.DocKeyCountMap[id].DataLen, len(doc.Data))
					}

					if slices.Contains(state.TroubleShoot.IdTrack.Actions, "checkForEmptyDoc") {
						if len(doc.HeaderFields) == 0 {
							log.Printf("******************** >>>>>>>>>>>>> Tracking[checkForEmptyDoc] doc.headerFields:%d, doc.data:%d", len(doc.HeaderFields), len(doc.Data))
							if state.TroubleShoot.TerminateAtFirstTrackError {
								log.Fatal("Terminating due to track error ....")
							}
						}
					}
				}
			}
		}
	*/
}
