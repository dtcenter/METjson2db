package core

import (

	//	"fmt"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/NOAA-GSL/MET-parser/pkg/structColumnDefs"
	"github.com/NOAA-GSL/MET-parser/pkg/structColumnTypes"
	"github.com/NOAA-GSL/METdatacb/pkg/state"
	// "github.com/NOAA-GSL/METdatacb/pkg/structColumnDefs"
	// "github.com/NOAA-GSL/METdatacb/pkg/structColumnTypes"
)

// init runs before main() is evaluated
func init() {
	slog.Debug("statToJSON:init()")
}

// dummy function to satisfy the function signature of getExternalDocForId
func getMissingExternalDocForId(id string) (map[string]interface{}, error) {
	// fmt.Println("getExternalDocForId called with id:", id)
	// Put your own code here in this method but always return this exact error if the document is not found
	state.METParserNewDocId = id
	return nil, fmt.Errorf("%s: %s", structColumnTypes.DOC_NOT_FOUND, id)
}

func statFileToCbDocMetParser(filepath string) (map[string]interface{}, error) {
	slog.Debug(fmt.Sprintf("statFileToCbDocMetParser(" + filepath + ")"))
	var doc map[string]interface{}
	var err error

	file, err := os.Open(filepath) // open the file
	if err != nil {
		slog.Error("error opening file", err)
	}
	defer file.Close()
	rawData, err := io.ReadAll(file)
	if err != nil {
		slog.Error("error reading file", err)
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
		doc, err = structColumnDefs.ParseLine(headerLine, dataLine, &state.CbDocs, filepath, getMissingExternalDocForId)
		if err != nil {
			slog.Error("Expected no error, got %v", err)

		} else if len(state.METParserNewDocId) > 0 {
			state.AsyncMergeDocFetchChannels[idxFetch] <- state.METParserNewDocId
			idxFetch++
			if idxFetch >= int(state.Conf.ThreadsMergeDocFetch) {
				idxFetch = 0
			}
		}
	}
	if doc == nil {
		slog.Error("Expected parsed document, got nil")
	}

	return doc, err
}
