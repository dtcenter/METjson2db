package main

import (
	"fmt"
	"log"
	// "github.com/couchbase/gocb/v2"
)

type StatToCbRun struct {
	fileStatus map[string]string // filename:status
	documents  map[string]string // id:doc
}

var statToCbRun = StatToCbRun{}

// init runs before main() is evaluated
func init() {
	log.Println("StatToCbRun:init()")
	statToCbRun.fileStatus = make(map[string]string)
	statToCbRun.documents = make(map[string]string)
}

func startProcessing(files []string) bool {
	log.Println("StatToCbRun:startProcessing(" + string(len(files)) + ")")

	for i := 0; i < len(files); i++ {
		statToCbRun.fileStatus[files[i]] = "processing"
	}

	// TODO: update/create db file document

	for file, status := range statToCbRun.fileStatus {
		fmt.Println(file, status)
		err := statFileToCbDoc(file)
		if err != nil {
			log.Println("Unable to process:" + file)
			statToCbRun.fileStatus[file] = "error"
		} else {
			statToCbRun.fileStatus[file] = "finished"
		}
	}

	// TODO: update/create db file document

	return true
}
