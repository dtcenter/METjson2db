package core

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/dtcenter/METjson2db/pkg/state"
	"github.com/dtcenter/METjson2db/pkg/storage"
	"github.com/dtcenter/METjson2db/pkg/telemetry"
)

// init runs before main() is evaluated
func init() {
	slog.Debug("StatToCbRun:init()")
	state.StatToCbRun.FileStatus = make(map[string]string)
	state.StatToCbRun.Documents = make(map[string]interface{})
}

// startProcessingFromProvider walks a StorageProvider and parses each stat file.
func startProcessingFromProvider(ctx context.Context, provider storage.StorageProvider) error {
	start := time.Now()
	fileCount := 0

	err := provider.Walk(ctx, func(name string, r io.Reader) error {
		fileCount++
		state.StatToCbRun.FileStatus[name] = "processing"

		fileCtx, fileSpan := telemetry.Tracer.Start(ctx, telemetry.SpanParseStatFile,
			telemetry.WithFileAttribute(name))

		_, parseErr := parseStatFileContent(fileCtx, name, r)
		if parseErr != nil {
			fileSpan.SetStatus(codes.Error, parseErr.Error())
			fileSpan.SetAttributes(attribute.String("error", parseErr.Error()))
			state.StatToCbRun.FileStatus[name] = "error"
			telemetry.FilesProcessed.Add(fileCtx, 1, telemetry.StatusError)
		} else {
			state.StatToCbRun.FileStatus[name] = "finished"
			telemetry.FilesProcessed.Add(fileCtx, 1, telemetry.StatusSuccess)
		}
		fileSpan.End()
		return nil
	})

	slog.DebugContext(ctx, fmt.Sprintf("%d files processed in: %d ms", fileCount, time.Since(start).Milliseconds()))
	return err
}
