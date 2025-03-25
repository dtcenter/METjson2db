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

	for file, status := range state.StatToCbRun.FileStatus {
		slog.Debug(fmt.Sprintf("%s,%s", file, status))
		_, err := statFileToCbDocMetParser(file)
		if err != nil {
			slog.Debug("Unable to process:" + file)
			state.StatToCbRun.FileStatus[file] = "error"
		} else {
			state.StatToCbRun.FileStatus[file] = "finished"
		}
	}

	slog.Debug(fmt.Sprintf("%d", len(files)) + " files processed in:" + fmt.Sprintf("%d", time.Since(start).Milliseconds()) + " ms")

	return true
}
