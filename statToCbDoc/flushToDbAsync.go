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
	doc := <-asynFilesChannels[threadIdx]
	log.Printf("flushToDbAsync(%s)", doc.headerFields["ID"].StringVal)
}
