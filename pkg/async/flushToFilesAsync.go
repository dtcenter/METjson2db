package async

import (
	"fmt"
	"log"
	"log/slog"

	// "github.com/couchbase/gocb/v2"

	"github.com/NOAA-GSL/METdatacb/pkg/state"
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
		if !ok || doc == nil {
			slog.Debug(fmt.Sprintf("\tflushToFilesAsync(%d), no documents in channel!", threadIdx))
			break
		}
		// slog.Info(fmt.Sprintf("FlushToFilesAsync(), doc:%v", doc))

		if len(doc) == 0 {
			slog.Debug(fmt.Sprintf("\tflushToFilesAsync(%d), end-marker received!", threadIdx))
			break
		}

		id := doc["id"].(string)
		state.CbDocMutexMap[id].Lock()

		/*
			docStr := []byte(doc.ToJSONString())
			fileName := state.Conf.OutputFolder + "/" + doc.HeaderFields["ID"].StringVal + ".json"
			err := os.WriteFile(fileName, docStr, 0o644)
			if err != nil {
				log.Printf("Error writing output:%s", fileName)
			}
			// log.Printf("flushToFilesAsync(%d), ID:%s", threadIdx, doc.headerFields["ID"].StringVal)
		*/
		state.CbDocMutexMap[id].Unlock()
	}
	slog.Debug(fmt.Sprintf("flushToFilesAsync(%d) doc count:%d, errors:%d", threadIdx, count, errors))
	returnDoc := make(map[string]interface{})
	// returnDoc.InitReturn(int64(count), int64(errors))
	state.AsyncFlushToFileChannels[threadIdx] <- returnDoc
}
