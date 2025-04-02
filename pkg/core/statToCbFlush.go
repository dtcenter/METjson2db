package core

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	// "github.com/couchbase/gocb/v2"

	"github.com/NOAA-GSL/METdatacb/pkg/state"
	"github.com/NOAA-GSL/METdatacb/pkg/types"
	"github.com/NOAA-GSL/METdatacb/pkg/utils"
)

// init runs before main() is evaluated
func init() {
	slog.Debug("StatToCbFlush:init()")
}

func StatToCbFlush(flushFinal bool) {
	slog.Info(fmt.Sprintf("statToCbFlush(flushFinal:%t, docs:%d, totalLinesProcessed:%d)",
		flushFinal, len(state.CbDocs), state.TotalLinesProcessed))

	/*
		See spec in readme, section:
		# Output location, configuration and logic
	*/
	flushCount := 0
	if state.Conf.RunNonThreaded {
		for id, _ := range state.CbDocs {
			slog.Info("id:" + id)
			//if flushFinal || int64(dataLen) >= int64(state.Conf.FlushToDbDataSectionMaxCount) {
			if state.Conf.RunMode == "CREATE_JSON_DOC_ARCHIVE" {
				flushToFiles(id)
			}
			if state.Conf.RunMode == "DIRECT_LOAD_TO_DB" {
				conn := utils.GetDbConnection(state.Credentials)
				flushToDb(conn, id)
			}
			//}
		}
	} else {
		// distribute docs to channels, round-robin, for async processing
		idxDb := 0
		// for id, doc
		for _, doc := range state.CbDocs {
			if state.Conf.RunMode == "DIRECT_LOAD_TO_DB" {
				state.AsyncFlushToDbChannels[idxDb] <- doc.(map[string]interface{})
				idxDb++
				if idxDb >= int(state.Conf.ThreadsDbUpload) {
					idxDb = 0
				}
			}
		}
	}
	slog.Debug("\tflushCount:", slog.Any("flushCount", flushCount))
}

func flushToFiles(id string) {
	slog.Debug("flushToFiles(" + id + ")")

	doc := state.CbDocs[id]

	docStr := utils.DocPrettyPrint(doc.(map[string]interface{}))
	fileName := state.Conf.JsonArchiveFilePathAndPrefix + time.Now().Format(time.RFC3339) + id + ".json"
	err := os.WriteFile(fileName, []byte(docStr), 0o644)
	if err != nil {
		slog.Debug("Error writing output:" + fileName)
	}
}

func flushToDb(conn types.CbConnection, id string) {
	slog.Info("flushToDb(" + id + ")")

	doc := state.CbDocs[id].(map[string]interface{})
	slog.Debug(fmt.Sprintf("%v", doc))

	var anyJson = doc

	if anyJson["data"] == nil || len(anyJson) == 0 {
		slog.Debug("NULL document:" + id)
		return
	}

	if state.Conf.OverWriteData {
		// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
		_, err := conn.Collection.Upsert(id, anyJson, nil)
		if err != nil {
			slog.Error(err.Error())
		}
	} else {
		slog.Error("No merge supported in write to files mode!")
	}
}
