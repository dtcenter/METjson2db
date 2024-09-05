package main

import (
	"log"
	// "github.com/couchbase/gocb/v2"
)

// init runs before main() is evaluated
func init() {
	log.Println("flushToDbAsync:init()")
}

func flushToDbAsync(threadIdx int) {
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
		log.Printf("flushToDbAsync(%d), ID:%s", threadIdx, doc.headerFields["ID"].StringVal)
	}
}
