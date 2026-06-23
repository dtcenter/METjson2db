package core

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

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

func statFileToCbDocMetParser(filepath string) (map[string]interface{}, error) {
	slog.Debug("statFileToCbDocMetParser(" + filepath + ")")

	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("opening file %s: %w", filepath, err)
	}
	defer file.Close()

	return parseStatFileContent(filepath, file)
}

// parseStatFileContent parses stat file content from any io.Reader.
func parseStatFileContent(name string, r io.Reader) (map[string]interface{}, error) {
	var doc map[string]interface{}
	var err error

	rawData, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", name, err)
	}
	lines := strings.Split(string(rawData), "\n")
	headerLine := lines[0]

	idxFetch := 0
	for line := range lines {
		if line == 0 || lines[line] == "" {
			continue
		}
		dataLine := lines[line]
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

	return doc, err
}
