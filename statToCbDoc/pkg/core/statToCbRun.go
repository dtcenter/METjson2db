package core

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
	log.Printf("startProcessing(%d)", len(files))

	// log.Printf("files:\n%v", files)

	for i := 0; i < len(files); i++ {
		if len(files[i]) > 0 {
			statToCbRun.fileStatus[files[i]] = "processing"
		}
	}

	// TODO: update/create db file document

	start := time.Now()

	if conf.ThreadsFileProcessor <= 1 {
		for file, status := range statToCbRun.fileStatus {
			log.Printf(file, status)
			err := statFileToCbDoc(file)
			if err != nil {
				log.Println("Unable to process:" + file)
				statToCbRun.fileStatus[file] = "error"
			} else {
				statToCbRun.fileStatus[file] = "finished"
			}
		}
	} else {
		log.Fatal("Unimplemented feature, threadsFileProcessor > 1    (FOR A FUTURE RELEASE!!!!)")
		/*
			// distribute files to channels, round-robin, for async processing
			idx := 0
			for file, status := range statToCbRun.fileStatus {
				log.Printf(file, status)
				asyncFileProcessorChannels[idx] <- file
				idx++
				if idx >= int(conf.ThreadsFileProcessor) {
					idx = 0
				}
			}
			for fi := 0; fi < int(conf.ThreadsFileProcessor); fi++ {
				asyncFileProcessorChannels[fi] <- "end"
			}
			asyncWaitGroupFileProcessor.Wait()
			log.Printf("asyncWaitGroupFileProcessor finished!")
		*/
	}

	log.Printf(fmt.Sprintf("%d", len(files)) + " files processed in:" + fmt.Sprintf("%d", time.Since(start).Milliseconds()) + " ms")

	// TODO: update/create db file document

	return true
}
