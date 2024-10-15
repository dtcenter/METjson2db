package main

import (
	"encoding/json"
	"log"
	"os"
	"slices"

	"golang.org/x/exp/maps"
	// "github.com/couchbase/gocb/v2"
)

// init runs before main() is evaluated
func init() {
	log.Println("StatToCbFlush:init()")
}

func statToCbFlush(flushFinal bool) {
	log.Printf("statToCbFlush(flushFinal:%t, docs:%d, totalLinesProcessed:%d, FlushToDbDataSectionMaxCount:%d)",
		flushFinal, len(cbDocs),
		totalLinesProcessed, conf.FlushToDbDataSectionMaxCount)

	/*
		See spec in readme, section:
		# Output location, configuration and logic
	*/
	flushCount := 0
	if conf.RunNonThreaded {
		conn := getDbConnection(credentials)
		for id, doc := range cbDocs {
			headerLen := len(doc.headerFields)
			dataLen := len(maps.Keys(doc.data))
			if conf.UpdateOnlyOnDocKeyCountChange && headerLen == docKeyCountMap[doc.headerFields["ID"].StringVal].HeaderLen && dataLen == docKeyCountMap[doc.headerFields["ID"].StringVal].DataLen {
				continue
			}
			if flushFinal || int64(dataLen) >= int64(conf.FlushToDbDataSectionMaxCount) {
				if conf.WriteJSONsToFile {
					flushToFiles(id)
				}
				if conf.UploadToDb {
					flushToDb(conn, id)
				}
			}
			docKeyCountMap[id] = DocKeyCounts{len(doc.headerFields), len(doc.data)}
		}
	} else {
		// distribute docs to channels, round-robin, for async processing
		idxFiles := 0
		idxDb := 0
		// for id, doc
		for _, doc := range cbDocs {
			doc.mutex.RLock()
			docKeyCountMapMutex.RLock()
			headerLen := len(doc.headerFields)
			dataLen := len(maps.Keys(doc.data))
			if conf.UpdateOnlyOnDocKeyCountChange && headerLen == docKeyCountMap[doc.headerFields["ID"].StringVal].HeaderLen && dataLen == docKeyCountMap[doc.headerFields["ID"].StringVal].DataLen {
				doc.mutex.RUnlock()
				docKeyCountMapMutex.RUnlock()
				continue
			}
			flushed := doc.flushed
			doc.mutex.RUnlock()
			docKeyCountMapMutex.RUnlock()
			if flushFinal || (int64(dataLen) >= conf.FlushToDbDataSectionMaxCount && !flushed) {
				flushCount++
				if conf.WriteJSONsToFile {
					asynFlushToFileChannels[idxFiles] <- doc
					idxFiles++
					if idxFiles >= int(conf.ThreadsWriteToDisk) {
						idxFiles = 0
					}
				}
				if conf.UploadToDb {
					asynFlushToDbChannels[idxDb] <- doc
					idxDb++
					if idxDb >= int(conf.ThreadsDbUpload) {
						idxDb = 0
					}
				}
			}
		}
	}
	log.Printf("\tflushCount:%d", flushCount)
}

func flushToFiles(id string) {
	log.Printf("flushToFiles(%s)", id)

	doc := cbDocs[id]
	log.Printf("data keys:%v", maps.Keys(doc.data))

	docStr := []byte(doc.toJSONString())
	fileName := conf.OutputFolder + "/" + id + ".json"
	err := os.WriteFile(fileName, docStr, 0644)
	if err != nil {
		log.Printf("Error writing output:%s", fileName)
	}
}

func flushToDb(conn CbConnection, id string) {
	log.Printf("flushToDb(%s)", id)

	doc := cbDocs[id]
	log.Printf("data keys:%v", maps.Keys(doc.data))

	/*
		TODO:
		1. Merge if conf[overWriteData] == false otherwise ovewrite doc
		2. Make flushToDb threaded and async
	*/

	var anyJson map[string]interface{}
	json.Unmarshal([]byte(doc.toJSONString()), &anyJson)

	// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
	_, err := conn.Collection.Upsert(doc.headerFields["ID"].StringVal, anyJson, nil)
	if err != nil {
		log.Fatal(err)
	}

	if troubleShoot.EnableTrackContextFlushToDb {
		for i := 0; i < len(troubleShoot.IdTrack.IdList); i++ {
			if id == troubleShoot.IdTrack.IdList[i] || troubleShoot.IdTrack.IdList[i] == "*" {
				if slices.Contains(troubleShoot.IdTrack.Actions, "logJSON") {
					log.Printf(">>>>>>>>>>>>> Tracking[logJSON] doc:\n%s\n", doc.toJSONString())
				}
				if slices.Contains(troubleShoot.IdTrack.Actions, "verifyWithDbRead") {
					/*
						sqlStr := "SELECT c FROM metdata._default.MET_default AS c WHERE c.ID = \"" + id + "\""
						log.Printf(">>>>>>>>>>>>> Tracking[verifyWithDbRead], SQL:\n%s", sqlStr)
						result := queryWithSQLStringMAP(conn.Scope, sqlStr)
						m := result[0].(map[string]interface{})
						dbReadDoc := m["c"].(map[string]interface{})
					*/
					dbReadDoc := getDocWithId(conn.Collection, id)
					trackError := false
					if dbReadDoc == nil {
						log.Printf(">>>>>>>>>>>>> Tracking[verifyWithDbRead] ID:%s, null data!!!", id)
						trackError = true
					} else {
						log.Printf(">>>>>>>>>>>>> Tracking[verifyWithDbRead] ID:%s, headerFields:[cur:%d, db:%d], data:[cur:%d, db:%d]", dbReadDoc["ID"],
							len(doc.headerFields), len(dbReadDoc)-1, len(doc.data), len(dbReadDoc["data"].(map[string]interface{})))
						if len(doc.headerFields) != (len(dbReadDoc)-1) || len(doc.data) != len(dbReadDoc["data"].(map[string]interface{})) {
							log.Printf("******************** >>>>>>>>>>>>> Tracking[verifyWithDbRead], data mismatch: ID:%s, headerFields:[cur:%d, db:%d], data:[cur:%d, db:%d]", dbReadDoc["ID"],
								len(doc.headerFields), len(dbReadDoc)-1, len(doc.data), len(dbReadDoc["data"].(map[string]interface{})))
							trackError = true
						}
					}
					if trackError && troubleShoot.TerminateAtFirstTrackError {
						log.Fatal("Terminating due to track error ....")
					}
				}

				if slices.Contains(troubleShoot.IdTrack.Actions, "trackDataKeyCount") {
					log.Printf(">>>>>>>>>>>>> Tracking[trackDataKeyCount] doc.headerFields:%d, doc.data:[prev:%d, cur:%d]", len(doc.headerFields), docKeyCountMap[id].DataLen, len(doc.data))
				}

				if slices.Contains(troubleShoot.IdTrack.Actions, "checkForEmptyDoc") {
					if len(doc.headerFields) == 0 {
						log.Printf("******************** >>>>>>>>>>>>> Tracking[checkForEmptyDoc] doc.headerFields:%d, doc.data:%d", len(doc.headerFields), len(doc.data))
						if troubleShoot.TerminateAtFirstTrackError {
							log.Fatal("Terminating due to track error ....")
						}
					}
				}
			}
		}
	}
}
