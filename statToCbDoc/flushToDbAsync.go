package main

import (
	"encoding/json"
	"log"
	// "github.com/couchbase/gocb/v2"
)

// init runs before main() is evaluated
func init() {
	log.Println("flushToDbAsync:init()")
}

func flushToDbAsync(threadIdx int, conn CbConnection) {
	count := 0
	for {
		doc, ok := <-asynDbChannels[threadIdx]
		if len(doc.headerFields) == 0 {
			log.Printf("\tflushToDbAsync(%d), end-marker received!", threadIdx)
			break
		}
		if !ok {
			log.Printf("\tflushToDbAsync(%d), no documents in channel!", threadIdx)
			break
		}
		//log.Printf("flushToDbAsync(%d), ID:%s", threadIdx, doc.headerFields["ID"].StringVal)

		var anyJson map[string]interface{}
		json.Unmarshal([]byte(doc.toJSONString()), &anyJson)

		// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
		_, err := conn.Collection.Upsert(doc.headerFields["ID"].StringVal, anyJson, nil)
		if err != nil {
			log.Println(err)
		}
		count++
	}
	log.Printf("flushToDbAsync(%d) doc count:%d", threadIdx, count)
}
