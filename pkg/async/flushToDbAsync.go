package async

import (
	"encoding/json"
	"log"
	"slices"

	"github.com/NOAA-GSL/METdatacb/pkg/state"
	"github.com/NOAA-GSL/METdatacb/pkg/types"
	"github.com/NOAA-GSL/METdatacb/pkg/utils"
)

// init runs before main() is evaluated
func init() {
	log.Println("flushToDbAsync:init()")
}

func FlushToDbAsync(threadIdx int /*, conn CbConnection*/) {
	conn := utils.GetDbConnection(state.Credentials)
	count := 0
	errors := 0
	for {
		doc, ok := <-state.AsyncFlushToDbChannels[threadIdx]
		doc.Mutex.Lock()
		if len(doc.HeaderFields) == 0 {
			log.Printf("\tflushToDbAsync(%d), end-marker received!", threadIdx)
			doc.Mutex.Unlock()
			break
		}
		if !ok {
			log.Printf("\tflushToDbAsync(%d), no documents in channel!", threadIdx)
			break
		}
		// log.Printf("flushToDbAsync(%d), ID:%s", threadIdx, doc.headerFields["ID"].StringVal)

		var anyJson map[string]interface{}

		err := json.Unmarshal([]byte(doc.ToJSONString()), &anyJson)

		if err != nil || anyJson["data"] == nil || len(anyJson["data"].(map[string]interface{})) == 0 {
			// log.Printf("NULL document[%s], err:%v", doc.HeaderFields["ID"].StringVal, err)
			if err != nil {
				log.Printf("err:%v, doc:\n%s", err, doc.ToJSONString())
			}
			doc.Mutex.Unlock()
			continue
		}

		doc.Flushed = true
		id := doc.HeaderFields["ID"].StringVal

		/*
			https://docs.couchbase.com/go-sdk/current/howtos/kv-operations.html
			CAS - Compare and Swap (CAS)
		*/

		// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
		_, err = conn.Collection.Upsert(id, anyJson, nil)
		if err != nil {
			log.Println(err)
			log.Printf("******* Upsert error:ID:%s", id)
			doc.Flushed = false
		} else {
			count++

			state.DocKeyCountMapMutex.Lock()
			state.DocKeyCountMap[id] = types.DocKeyCounts{HeaderLen: len(doc.HeaderFields), DataLen: len(doc.Data)}
			state.DocKeyCountMapMutex.Unlock()

			if state.TroubleShoot.EnableTrackContextFlushToDb {
				for i := 0; i < len(state.TroubleShoot.IdTrack.IdList); i++ {
					if id == state.TroubleShoot.IdTrack.IdList[i] || state.TroubleShoot.IdTrack.IdList[i] == "*" {
						if slices.Contains(state.TroubleShoot.IdTrack.Actions, "logJSON") {
							log.Printf(">>>>>>>>>>>>> Tracking[logJSON] doc:\n%s\n", doc.ToJSONString())
						}
						if slices.Contains(state.TroubleShoot.IdTrack.Actions, "verifyWithDbRead") {
							/*
								sqlStr := "SELECT c FROM metdata._default.MET_default AS c WHERE c.ID = \"" + id + "\""
								log.Printf(">>>>>>>>>>>>> Tracking[verifyWithDbRead], SQL:\n%s", sqlStr)
								result := queryWithSQLStringMAP(conn.Scope, sqlStr)
								m := result[0].(map[string]interface{})
								dbReadDoc := m["c"].(map[string]interface{})
							*/
							dbReadDoc := utils.GetDocWithId(conn.Collection, id)
							if dbReadDoc == nil {
								log.Printf(">>>>>>>>>>>>> Tracking[verifyWithDbRead] ID:%s, null data!!!", id)
								if state.TroubleShoot.TerminateAtFirstTrackError {
									log.Fatal("Terminating due to track error ....")
								}
							} else {
								log.Printf(">>>>>>>>>>>>> Tracking[verifyWithDbRead] ID:%s, headerFields:[cur:%d, db:%d], data:[cur:%d, db:%d]", dbReadDoc["ID"],
									len(doc.HeaderFields), len(dbReadDoc)-1, len(doc.Data), len(dbReadDoc["data"].(map[string]interface{})))
								if len(doc.HeaderFields) != (len(dbReadDoc)-1) || len(doc.Data) != len(dbReadDoc["data"].(map[string]interface{})) {
									log.Printf("******************** >>>>>>>>>>>>> Tracking[verifyWithDbRead], data mismatch: ID:%s, headerFields:[cur:%d, db:%d], data:[cur:%d, db:%d]", dbReadDoc["ID"],
										len(doc.HeaderFields), len(dbReadDoc)-1, len(doc.Data), len(dbReadDoc["data"].(map[string]interface{})))
									if state.TroubleShoot.TerminateAtFirstTrackError {
										log.Fatal("Terminating due to track error ....")
									}
								}
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
		}
		doc.Mutex.Unlock()
	}
	log.Printf("flushToDbAsync(%d) doc count:%d, errors:%d", threadIdx, count, errors)
	returnDoc := types.CbDataDocument{}
	returnDoc.InitReturn(int64(count), int64(errors))
	state.AsyncFlushToDbChannels[threadIdx] <- returnDoc
}
