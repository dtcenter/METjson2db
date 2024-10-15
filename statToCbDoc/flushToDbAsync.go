package main

import (
	"encoding/json"
	"log"
	"slices"
)

// init runs before main() is evaluated
func init() {
	log.Println("flushToDbAsync:init()")
}

func flushToDbAsync(threadIdx int /*, conn CbConnection*/) {
	conn := getDbConnection(credentials)
	count := 0
	errors := 0
	for {
		doc, ok := <-asynFlushToDbChannels[threadIdx]
		doc.mutex.Lock()
		if len(doc.headerFields) == 0 {
			log.Printf("\tflushToDbAsync(%d), end-marker received!", threadIdx)
			doc.mutex.Unlock()
			break
		}
		if !ok {
			log.Printf("\tflushToDbAsync(%d), no documents in channel!", threadIdx)
			break
		}
		// log.Printf("flushToDbAsync(%d), ID:%s", threadIdx, doc.headerFields["ID"].StringVal)

		var anyJson map[string]interface{}

		json.Unmarshal([]byte(doc.toJSONString()), &anyJson)
		doc.flushed = true
		id := doc.headerFields["ID"].StringVal

		/*
			https://docs.couchbase.com/go-sdk/current/howtos/kv-operations.html
			CAS - Compare and Swap (CAS)
		*/

		// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
		_, err := conn.Collection.Upsert(id, anyJson, nil)
		if err != nil {
			log.Println(err)
			log.Printf("******* Upsert error:ID:%s", id)
			doc.flushed = false
		} else {
			count++

			docKeyCountMapMutex.Lock()
			docKeyCountMap[id] = DocKeyCounts{len(doc.headerFields), len(doc.data)}
			docKeyCountMapMutex.Unlock()

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
							if dbReadDoc == nil {
								log.Printf(">>>>>>>>>>>>> Tracking[verifyWithDbRead] ID:%s, null data!!!", id)
								if troubleShoot.TerminateAtFirstTrackError {
									log.Fatal("Terminating due to track error ....")
								}
							} else {
								log.Printf(">>>>>>>>>>>>> Tracking[verifyWithDbRead] ID:%s, headerFields:[cur:%d, db:%d], data:[cur:%d, db:%d]", dbReadDoc["ID"],
									len(doc.headerFields), len(dbReadDoc)-1, len(doc.data), len(dbReadDoc["data"].(map[string]interface{})))
								if len(doc.headerFields) != (len(dbReadDoc)-1) || len(doc.data) != len(dbReadDoc["data"].(map[string]interface{})) {
									log.Printf("******************** >>>>>>>>>>>>> Tracking[verifyWithDbRead], data mismatch: ID:%s, headerFields:[cur:%d, db:%d], data:[cur:%d, db:%d]", dbReadDoc["ID"],
										len(doc.headerFields), len(dbReadDoc)-1, len(doc.data), len(dbReadDoc["data"].(map[string]interface{})))
									if troubleShoot.TerminateAtFirstTrackError {
										log.Fatal("Terminating due to track error ....")
									}
								}
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
		doc.mutex.Unlock()
	}
	log.Printf("flushToDbAsync(%d) doc count:%d, errors:%d", threadIdx, count, errors)
	returnDoc := CbDataDocument{}
	returnDoc.initReturn(int64(count), int64(errors))
	asynFlushToDbChannels[threadIdx] <- returnDoc
}
