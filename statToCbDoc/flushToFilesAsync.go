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
	count := 0
	errors := 0
	for {
		doc, ok := <-asyncFlushToFileChannels[threadIdx]
		doc.mutex.Lock()
		if len(doc.headerFields) == 0 {
			log.Printf("\tflushToFilesAsync(%d), end-marker received!", threadIdx)
			doc.mutex.Unlock()
			break
		}
		if !ok {
			log.Printf("\tflushToFilesAsync(%d), no documents in channel!", threadIdx)
			break
		}
		//log.Printf("flushToFilesAsync(%d), ID:%s", threadIdx, doc.headerFields["ID"].StringVal)
		doc.mutex.Unlock()
	}
	log.Printf("flushToFilesAsync(%d) doc count:%d, errors:%d", threadIdx, count, errors)
	returnDoc := CbDataDocument{}
	returnDoc.initReturn(int64(count), int64(errors))
	asyncFlushToFileChannels[threadIdx] <- returnDoc
}
