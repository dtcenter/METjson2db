package async

import (
	"log"
	"os"

	// "github.com/couchbase/gocb/v2"

	"github.com/NOAA-GSL/METdatacb/pkg/state"
	"github.com/NOAA-GSL/METdatacb/pkg/types"
)

// init runs before main() is evaluated
func init() {
	log.Println("flushToFilesAsync:init()")
}

func FlushToFilesAsync(threadIdx int) {
	count := 0
	errors := 0
	for {
		doc, ok := <-state.AsyncFlushToFileChannels[threadIdx]
		doc.Mutex.Lock()
		if len(doc.HeaderFields) == 0 {
			log.Printf("\tflushToFilesAsync(%d), end-marker received!", threadIdx)
			doc.Mutex.Unlock()
			break
		}
		if !ok {
			log.Printf("\tflushToFilesAsync(%d), no documents in channel!", threadIdx)
			break
		}
		docStr := []byte(doc.ToJSONString())
		fileName := state.Conf.OutputFolder + "/" + doc.HeaderFields["ID"].StringVal + ".json"
		err := os.WriteFile(fileName, docStr, 0644)
		if err != nil {
			log.Printf("Error writing output:%s", fileName)
		}
		//log.Printf("flushToFilesAsync(%d), ID:%s", threadIdx, doc.headerFields["ID"].StringVal)
		doc.Mutex.Unlock()
	}
	log.Printf("flushToFilesAsync(%d) doc count:%d, errors:%d", threadIdx, count, errors)
	returnDoc := types.CbDataDocument{}
	returnDoc.InitReturn(int64(count), int64(errors))
	state.AsyncFlushToFileChannels[threadIdx] <- returnDoc
}
