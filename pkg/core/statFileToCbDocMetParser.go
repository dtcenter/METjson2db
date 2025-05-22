package core

import (

	//	"fmt"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/NOAA-GSL/METstat2json/pkg/metLineTypeParser"

	// "github.com/NOAA-GSL/METjson2db/pkg/structColumnDefs"
	// "github.com/NOAA-GSL/METjson2db/pkg/structColumnTypes"

	"github.com/NOAA-GSL/METjson2db/pkg/state"
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
	return nil, fmt.Errorf("%s: %s", metLineTypeParser.DOC_NOT_FOUND, id)
}

func statFileToCbDocMetParser(filepath string) (map[string]interface{}, error) {
	slog.Debug(fmt.Sprintf("statFileToCbDocMetParser(" + filepath + ")"))
	var doc map[string]interface{}
	var err error

	file, err := os.Open(filepath) // open the file
	if err != nil {
		slog.Error("error opening file:", slog.Any("error", err))
	}
	defer file.Close()
	rawData, err := io.ReadAll(file)
	if err != nil {
		slog.Error("error reading file:", filepath, slog.Any("error", err))
	}
	lines := strings.Split(string(rawData), "\n")
	headerLine := lines[0]

	// distribute ids to fetch channels, round-robin, for async processing
	idxFetch := 0
	for line := range lines {
		if line == 0 || lines[line] == "" {
			continue
		}
		dataLine := lines[line]
		state.METParserNewDocId = ""
		// TODO: from command line, document it
		doc, err = metLineTypeParser.ParseLine(state.LoadSpec.DatasetName, headerLine, dataLine, &state.CbDocs, filepath, getMissingExternalDocForId)
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
