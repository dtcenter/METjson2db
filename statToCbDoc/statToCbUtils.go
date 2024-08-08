package main

import (
	"log"
	// "github.com/couchbase/gocb/v2"
)

// init runs before main() is evaluated
func init() {
	log.Println("StatToCbUtils:init()")
}

func createTestcbDocument() CbDataDocument {
	doc := CbDataDocument{}
	doc.init()

	return doc
}
