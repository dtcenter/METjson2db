package async

import (
	"fmt"
	"log"
	// "github.com/couchbase/gocb/v2"

	"github.com/NOAA-GSL/METdatacb/statToCbDoc/pkg/main"
)

// init runs before main() is evaluated
func init() {
	log.Println("fileProcessorAsync:init()")
}

func fileProcessorAsync(threadIdx int) {
	count := 0
	errors := 0
	for {
		file, ok := <-main.asyncFileProcessorChannels[threadIdx]
		if file == "end" {
			log.Printf("\tfileProcessorAsync(%d), end-marker received!", threadIdx)
			break
		}
		if !ok {
			log.Printf("\tfileProcessorAsync(%d), no files in channel!", threadIdx)
			break
		}
		log.Printf("fileProcessorAsync(%d), file:%s", threadIdx, file)
		err := statFileToCbDoc(file)
		if err != nil {
			log.Println("Unable to process:" + file)
			statToCbRun.fileStatus[file] = "error"
			errors++
		} else {
			statToCbRun.fileStatus[file] = "finished"
			count++
		}
	}
	log.Printf("fileProcessorAsync(%d) file count:%d, errors:%d", threadIdx, count, errors)
	asyncFileProcessorChannels[threadIdx] <- fmt.Sprintf("endReturn:%d:%d", count, errors)
}
