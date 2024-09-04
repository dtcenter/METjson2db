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
	doc := <-asynFilesChannels[threadIdx]
	log.Printf("flushToFilesAsync(%s)", doc.headerFields["ID"].StringVal)
}
