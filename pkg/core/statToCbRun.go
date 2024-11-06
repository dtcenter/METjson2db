package core

import (
	"fmt"
	"log"
	"time"

	// "github.com/couchbase/gocb/v2"

	"github.com/NOAA-GSL/METdatacb/pkg/state"
	"github.com/NOAA-GSL/METdatacb/pkg/types"
)

// init runs before main() is evaluated
func init() {
	log.Println("StatToCbRun:init()")
	state.StatToCbRun.FileStatus = make(map[string]string)
	state.StatToCbRun.Documents = make(map[string]types.CbDataDocument)
}

func StartProcessing(files []string) bool {
	log.Printf("startProcessing(%d)", len(files))

	// log.Printf("files:\n%v", files)

	for i := 0; i < len(files); i++ {
		if len(files[i]) > 0 {
			state.StatToCbRun.FileStatus[files[i]] = "processing"
		}
	}

	// TODO: update/create db file document

	start := time.Now()

	if state.Conf.ThreadsFileProcessor <= 1 {
		for file, status := range state.StatToCbRun.FileStatus {
			log.Printf(file, status)
			err := StatFileToCbDoc(file)
			if err != nil {
				log.Println("Unable to process:" + file)
				state.StatToCbRun.FileStatus[file] = "error"
			} else {
				state.StatToCbRun.FileStatus[file] = "finished"
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
