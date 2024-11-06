package async

import (
	"fmt"
	"log"

	// "github.com/couchbase/gocb/v2"

	"github.com/NOAA-GSL/METdatacb/pkg/core"
	"github.com/NOAA-GSL/METdatacb/pkg/state"
)

// init runs before main() is evaluated
func init() {
	log.Println("fileProcessorAsync:init()")
}

func FileProcessorAsync(threadIdx int) {
	count := 0
	errors := 0
	for {
		file, ok := <-state.AsyncFileProcessorChannels[threadIdx]
		if file == "end" {
			log.Printf("\tfileProcessorAsync(%d), end-marker received!", threadIdx)
			break
		}
		if !ok {
			log.Printf("\tfileProcessorAsync(%d), no files in channel!", threadIdx)
			break
		}
		log.Printf("fileProcessorAsync(%d), file:%s", threadIdx, file)
		err := core.StatFileToCbDoc(file)
		if err != nil {
			log.Println("Unable to process:" + file)
			state.StatToCbRun.FileStatus[file] = "error"
			errors++
		} else {
			state.StatToCbRun.FileStatus[file] = "finished"
			count++
		}
	}
	log.Printf("fileProcessorAsync(%d) file count:%d, errors:%d", threadIdx, count, errors)
	state.AsyncFileProcessorChannels[threadIdx] <- fmt.Sprintf("endReturn:%d:%d", count, errors)
}
