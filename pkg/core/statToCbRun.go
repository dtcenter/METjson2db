package core

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/dtcenter/METjson2db/pkg/state"
	"github.com/dtcenter/METjson2db/pkg/storage"
)

// init runs before main() is evaluated
func init() {
	slog.Debug("StatToCbRun:init()")
	state.StatToCbRun.FileStatus = make(map[string]string)
	state.StatToCbRun.Documents = make(map[string]interface{})
}

// StartProcessing processes files from a list of paths (backward-compatible).
func StartProcessing(files []string) bool {
	provider := storage.NewLocalProvider(files)
	err := StartProcessingFromProvider(context.Background(), provider)
	return err == nil
}

// StartProcessingFromProvider processes stat files from a StorageProvider.
func StartProcessingFromProvider(ctx context.Context, provider storage.StorageProvider) error {
	start := time.Now()
	fileCount := 0

	err := provider.Walk(ctx, func(name string, r io.Reader) error {
		fileCount++
		state.StatToCbRun.FileStatus[name] = "processing"
		slog.Debug(fmt.Sprintf("%s,%s", name, "processing"))

		_, parseErr := parseStatFileContent(name, r)
		if parseErr != nil {
			slog.Debug("Unable to process:" + name)
			state.StatToCbRun.FileStatus[name] = "error"
		} else {
			state.StatToCbRun.FileStatus[name] = "finished"
		}
		return nil
	})

	slog.Debug(fmt.Sprintf("%d files processed in: %d ms", fileCount, time.Since(start).Milliseconds()))
	return err
}
