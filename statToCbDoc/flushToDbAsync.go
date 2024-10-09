package main

import (
	"encoding/json"
	"log"
	"slices"

	"github.com/couchbase/gocb/v2"
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
		if len(doc.headerFields) == 0 {
			log.Printf("\tflushToDbAsync(%d), end-marker received!", threadIdx)
			break
		}
		if !ok {
			log.Printf("\tflushToDbAsync(%d), no documents in channel!", threadIdx)
			break
		}
		// log.Printf("flushToDbAsync(%d), ID:%s", threadIdx, doc.headerFields["ID"].StringVal)

		var anyJson map[string]interface{}

		doc.mutex.Lock()
		json.Unmarshal([]byte(doc.toJSONString()), &anyJson)
		doc.flushed = true
		id := doc.headerFields["ID"].StringVal

		// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
		_, err := conn.Collection.Upsert(id, anyJson, nil)
		if err != nil {
			log.Println(err)
			log.Printf("******* Upsert error:ID:%s", id)
			doc.flushed = false
		} else {
			count++

			if troubleShoot.EnableTrackContextFlushToDb {
				for i := 0; i < len(troubleShoot.IdTrack.IdList); i++ {
					if id == troubleShoot.IdTrack.IdList[i] || troubleShoot.IdTrack.IdList[i] == "*" {
						if slices.Contains(troubleShoot.IdTrack.Actions, "logJSON") {
							log.Printf(">>>>>>>>>>>>> Tracking[logJSON] doc:\n%s\n", doc.toJSONString())
						}
						if slices.Contains(troubleShoot.IdTrack.Actions, "verifyWithDbRead") {
							sqlStr := "SELECT c FROM metdata._default.MET_default AS c WHERE c.ID = \"" + id + "\""
							log.Printf(">>>>>>>>>>>>> Tracking[verifyWithDbRead], SQL:\n%s", sqlStr)
							queryResult, err := conn.Scope.Query(sqlStr, &gocb.QueryOptions{Adhoc: true})
							if err != nil {
								log.Fatal(err)
							} else {
								printQueryResult(queryResult)
							}
							log.Printf("Tracking[verifyWithDbRead] doc:\n%s\n", doc.toJSONString())
						}

						if slices.Contains(troubleShoot.IdTrack.Actions, "trackDataKeyCount") {
							log.Printf(">>>>>>>>>>>>> Tracking[trackDataKeyCount] doc.headerFields:%d, doc.data:[prev:%d, cur:%d]", len(doc.headerFields), docKeyCountMap[id].DataLen, len(doc.data))
							docKeyCountMap[id] = DocKeyCounts{len(doc.headerFields), len(doc.data)}
						}

						if slices.Contains(troubleShoot.IdTrack.Actions, "checkForEmptyDoc") {
							log.Printf(">>>>>>>>>>>>> Tracking[checkForEmptyDoc] doc.headerFields:%d, doc.data:%d", len(doc.headerFields), len(doc.data))
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
