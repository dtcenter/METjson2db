package main

import (
	"log"
	// "github.com/couchbase/gocb/v2"
)

// init runs before main() is evaluated
func init() {
	log.Println("flushToFilesAsync:init()")
}

func flushToFilesAsync(threadIdx int) {
	for {
		doc, ok := <-asynFilesChannels[threadIdx]
		if len(doc.headerFields) == 0 {
			log.Printf("\tflushToFilesAsync(%d), end-marker received!", threadIdx)
			break
		}
		if !ok {
			log.Printf("\tflushToFilesAsync(%d), no documents in channel!", threadIdx)
			break
		}
		log.Printf("flushToFilesAsync(%d), ID:%s", threadIdx, doc.headerFields["ID"].StringVal)
	}
}
