package main

import (
	"fmt"
	"log"
	"time"
	// "github.com/couchbase/gocb/v2"
)

var statToCbRun = StatToCbRun{}

type StatToCbRun struct {
	fileStatus map[string]string         // filename:status
	documents  map[string]CbDataDocument // id:doc
}

// init runs before main() is evaluated
func init() {
	log.Println("StatToCbRun:init()")
	statToCbRun.fileStatus = make(map[string]string)
	statToCbRun.documents = make(map[string]CbDataDocument)
}

func startProcessing(files []string) bool {
	log.Println("startProcessing(" + fmt.Sprint(len(files)) + ")")
	fmt.Println("files:\n", files)

	for i := 0; i < len(files); i++ {
		if len(files[i]) > 0 {
			statToCbRun.fileStatus[files[i]] = "processing"
		}
	}

	// TODO: update/create db file document

	start := time.Now()
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
	log.Printf(fmt.Sprintf("%d", len(files)) + " files processed in:" + fmt.Sprintf("%d", time.Since(start).Milliseconds()) + " ms")

	// TODO: update/create db file document

	return true
}
