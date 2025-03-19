package async

import (
	"fmt"
	"log/slog"

	// "github.com/couchbase/gocb/v2"

	"github.com/NOAA-GSL/METdatacb/pkg/state"
)

// init runs before main() is evaluated
func init() {
	slog.Debug("fileProcessorAsync:init()")
}

func FileProcessorAsync(threadIdx int) {
	count := 0
	errors := 0
	for {
		file, ok := <-state.AsyncFileProcessorChannels[threadIdx]
		if file == "end" {
			slog.Debug(fmt.Sprintf("\tfileProcessorAsync(%d), end-marker received!", threadIdx))
			break
		}
		if !ok {
			slog.Debug(fmt.Sprintf("\tfileProcessorAsync(%d), no files in channel!", threadIdx))
			break
		}
		slog.Debug(fmt.Sprintf("fileProcessorAsync(%d), file:%s", threadIdx, file))
		/*
			err := core.StatFileToCbDoc(file)
			if err != nil {
				slog.Debug("Unable to process:" + file)
				state.StatToCbRun.FileStatus[file] = "error"
				errors++
			} else {
				state.StatToCbRun.FileStatus[file] = "finished"
				count++
			}
		*/
	}
	slog.Debug(fmt.Sprintf("fileProcessorAsync(%d) file count:%d, errors:%d", threadIdx, count, errors))
	state.AsyncFileProcessorChannels[threadIdx] <- fmt.Sprintf("endReturn:%d:%d", count, errors)
}
