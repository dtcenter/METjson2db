package core

import (
	"fmt"
	"log/slog"
	"time"

	// "github.com/couchbase/gocb/v2"

	"github.com/NOAA-GSL/METdatacb/pkg/state"
)

// init runs before main() is evaluated
func init() {
	slog.Debug("StatToCbRun:init()")
	state.StatToCbRun.FileStatus = make(map[string]string)
	state.StatToCbRun.Documents = make(map[string]interface{})
}

func StartProcessing(files []string) bool {
	slog.Info(fmt.Sprintf("startProcessing(%d)", len(files)))

	// slog.Debug("files:\n%v", files)

	for i := 0; i < len(files); i++ {
		if len(files[i]) > 0 {
			state.StatToCbRun.FileStatus[files[i]] = "processing"
		}
	}

	// TODO: update/create db file document

	start := time.Now()

	if state.Conf.ThreadsFileProcessor <= 1 {
		for file, status := range state.StatToCbRun.FileStatus {
			slog.Debug(fmt.Sprintf("%s,%s", file, status))
			//err := StatFileToCbDoc(file)
			_, err := statFileToCbDocMetParser(file)
			if err != nil {
				slog.Debug("Unable to process:" + file)
				state.StatToCbRun.FileStatus[file] = "error"
			} else {
				state.StatToCbRun.FileStatus[file] = "finished"

				state.CbDocsMutex.RLock()

				/*
					for _, docR := range docList {
						doc := docR.(map[string]interface{})
						id := doc["id"].(string)
						// slog.Info("id:" + id)

						_, ok := state.CbDocMutexMap[id]
						if !ok {
							// state.CbDocs[id] = doc
							state.CbDocMutexMap[id] = &sync.RWMutex{}
						} else {

						}
					}
				*/

				state.CbDocsMutex.RUnlock()
			}
		}
	} else {
		slog.Error("Unimplemented feature, threadsFileProcessor > 1    (FOR A FUTURE RELEASE!!!!)")
		/*
			// distribute files to channels, round-robin, for async processing
			idx := 0
			for file, status := range statToCbRun.fileStatus {
				slog.Debug(file, status)
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
			slog.Debug("asyncWaitGroupFileProcessor finished!")
		*/
	}

	slog.Debug(fmt.Sprintf("%d", len(files)) + " files processed in:" + fmt.Sprintf("%d", time.Since(start).Milliseconds()) + " ms")

	// TODO: update/create db file document

	return true
}
