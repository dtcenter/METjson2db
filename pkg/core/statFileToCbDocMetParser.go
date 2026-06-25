package core

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"

	"github.com/dtcenter/METjson2db/pkg/state"
	"github.com/dtcenter/METstat2json/pkg/parser"
)

// init runs before main() is evaluated
func init() {
	slog.Debug("statToJSON:init()")
}

// dummy function to satisfy the function signature of getExternalDocForId
func getMissingExternalDocForId(id string) (map[string]interface{}, error) {
	// fmt.Println("getExternalDocForId called with id:", id)
	// Put your own code here in this method but always return this exact error if the document is not found
	slog.Debug(fmt.Sprintf("getMissingExternalDocForId(%v)", state.METParserNewDocId))
	state.METParserNewDocId = id
	return nil, fmt.Errorf("%s: %s", parser.DOC_NOT_FOUND, id)
}

// parseStatFileContent parses stat file content from any io.Reader.
func parseStatFileContent(name string, r io.Reader) (map[string]interface{}, error) {
	var doc map[string]interface{}
	var err error

	scanner := bufio.NewScanner(r)
	// Increase the maximum buffer size from the default 64kb to 1MB to handle extremely wide stat file lines
	// Pass nil to start small with the default 4kb buffer, but allow it to grow to 1MB
	const maxCapacity = 1024 * 1024
	scanner.Buffer(nil, maxCapacity)

	if !scanner.Scan() {
		return nil, fmt.Errorf("empty file or error reading header for %s: %w", name, scanner.Err())
	}
	headerLine := scanner.Text()

	idxFetch := 0
	for scanner.Scan() {
		dataLine := scanner.Text()
		if dataLine == "" {
			continue
		}

		state.METParserNewDocId = ""
		doc, err = parser.ParseLine(state.LoadSpec.DatasetName, headerLine, dataLine, &state.CbDocs, name, getMissingExternalDocForId)
		slog.Debug(fmt.Sprintf("OverWriteData:%v,METParserNewDocId:%v", state.LoadSpec.OverWriteData, state.METParserNewDocId))
		if err != nil {
			slog.Error("Expected no error, got:", slog.Any("error", err))
		} else if doc == nil {
			slog.Error("Expected parsed document, got nil, for line:" + dataLine)
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
