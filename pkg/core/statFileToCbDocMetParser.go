package core

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/dtcenter/METjson2db/pkg/state"
	"github.com/dtcenter/METjson2db/pkg/telemetry"
	"github.com/dtcenter/METstat2json/pkg/parser"
)

// init runs before main() is evaluated
func init() {
	slog.Debug("statToJSON:init()")
}

// parseStatFileContent parses stat file content from any io.Reader.
func parseStatFileContent(ctx context.Context, name string, r io.Reader) (map[string]interface{}, error) {
	var doc map[string]interface{}
	var err error

	// dummy function to satisfy the function signature of getExternalDocForId; closes over ctx so
	// the metric can be correlated with the active span instead of using context.Background().
	getMissingExternalDocForId := func(id string) (map[string]interface{}, error) {
		slog.DebugContext(ctx, fmt.Sprintf("getMissingExternalDocForId(%v)", state.METParserNewDocId))
		state.METParserNewDocId = id
		telemetry.MissingExternalDocRefs.Add(ctx, 1)
		return nil, fmt.Errorf("%s: %s", parser.DOC_NOT_FOUND, id)
	}

	scanner := bufio.NewScanner(r)
	const maxCapacity = 1024 * 1024
	scanner.Buffer(nil, maxCapacity)

	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			telemetry.StatFileParseErrors.Add(ctx, 1)
			return nil, fmt.Errorf("error reading header for %s: %w", name, err)
		}
		telemetry.StatFileParseErrors.Add(ctx, 1)
		return nil, fmt.Errorf("empty file: %s", name)
	}
	headerLine := scanner.Text()

	idxFetch := 0
	for scanner.Scan() {
		dataLine := scanner.Text()
		if dataLine == "" {
			telemetry.LinesSkipped.Add(ctx, 1)
			continue
		}

		telemetry.LinesParsed.Add(ctx, 1)
		state.METParserNewDocId = ""
		doc, err = parser.ParseLine(state.LoadSpec.DatasetName, headerLine, dataLine, &state.CbDocs, name, getMissingExternalDocForId)
		slog.Debug(fmt.Sprintf("OverWriteData:%v,METParserNewDocId:%v", state.LoadSpec.OverWriteData, state.METParserNewDocId))
		if err != nil {
			telemetry.StatFileParseErrors.Add(ctx, 1)
			slog.ErrorContext(ctx, "Expected no error, got:", slog.Any("error", err))
		} else if doc == nil {
			telemetry.StatFileParseErrors.Add(ctx, 1)
			slog.ErrorContext(ctx, "Expected parsed document, got nil, for line:"+dataLine)
		} else if !state.LoadSpec.OverWriteData && len(state.METParserNewDocId) > 0 {
			state.AsyncMergeDocFetchChannels[idxFetch] <- state.METParserNewDocId
			idxFetch++
			if idxFetch >= int(state.LoadSpec.ThreadsMergeDocFetch) {
				idxFetch = 0
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error scanning file %s: %w", name, err)
	}

	return doc, err
}
