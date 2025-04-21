package core

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/NOAA-GSL/METstat2json/pkg/metLineTypeParser"

	"github.com/NOAA-GSL/METjson2db/pkg/async"
	"github.com/NOAA-GSL/METjson2db/pkg/state"
	"github.com/NOAA-GSL/METjson2db/pkg/types"
)

// init runs before main() is evaluated
func init() {
	slog.Debug("ProcessInput:init()")
}

func ProcessInputFiles(inputFiles []string, preDbLoadCallback func()) error {
	slog.Info(fmt.Sprintf("ProcessInputFiles(%d)", len(inputFiles)))

	start := time.Now()
	state.StateReset()

	if state.Conf.RunMode == "DIRECT_LOAD_TO_DB" {
		if !state.Conf.RunNonThreaded {
			for di := 0; di < int(state.Conf.ThreadsDbUpload); di++ {
				di := di
				state.AsyncFlushToDbChannels = append(state.AsyncFlushToDbChannels, make(chan map[string]interface{}, state.Conf.ChannelBufferSizeNumberOfDocs))
				state.AsyncWaitGroupFlushToDb.Add(1)
				go func() {
					defer state.AsyncWaitGroupFlushToDb.Done()
					// conn := getDbConnection(credentials)
					async.FlushToDbAsync(di)
				}()
			}

			if false == state.Conf.OverWriteData {
				for di := 0; di < int(state.Conf.ThreadsMergeDocFetch); di++ {
					di := di
					state.AsyncMergeDocFetchChannels = append(state.AsyncMergeDocFetchChannels, make(chan string, state.Conf.ChannelBufferSizeNumberOfDocs))
					state.AsyncWaitGroupMergeDocFetch.Add(1)
					go func() {
						defer state.AsyncWaitGroupMergeDocFetch.Done()
						async.MergeDbDocFetchAsync(di)
					}()
				}
			}
		}
	}

	// slog.Error("Test exit!")

	StartProcessing(inputFiles)

	fileTotalCount := int64(0)
	fileTotalErrors := int64(0)
	dbTotalCount := int64(0)
	dbTotalErrors := int64(0)

	if state.Conf.RunMode == "DIRECT_LOAD_TO_DB" {
		if false == state.Conf.OverWriteData {
			for fi := 0; fi < int(state.Conf.ThreadsMergeDocFetch); fi++ {
				state.AsyncMergeDocFetchChannels[fi] <- "endMarker"
			}
			state.AsyncWaitGroupMergeDocFetch.Wait()
			slog.Info("AsyncWaitGroupMergeDocFetch finished!")
		}

		if preDbLoadCallback != nil {
			preDbLoadCallback()
		}
		StatToCbFlush(true)
		if !state.Conf.RunNonThreaded {
			slog.Debug("Waiting for threads to finish ...")

			// send end-marker doc to all channels
			endMarkerDoc := make(map[string]interface{})
			endMarkerDoc["endMarker"] = "endMarker"

			for di := 0; di < int(state.Conf.ThreadsDbUpload); di++ {
				state.AsyncFlushToDbChannels[di] <- endMarkerDoc
			}

			state.AsyncWaitGroupFlushToDb.Wait()
			slog.Debug("asyncWaitGroupFlushToDb finished!")

			// get return info from threads
			/*
				for fi := 0; fi < int(state.Conf.ThreadsWriteToDisk); fi++ {
					doc, ok := <-state.AsyncFlushToFileChannels[fi]
					if ok && len(doc.HeaderFields) > 0 {
						slog.Debug("\tflushToFilesAsync[%d], count:%d, errors:%d", fi, doc.HeaderFields["count"].IntVal, doc.HeaderFields["errors"].IntVal)
						fileTotalCount += doc.HeaderFields["count"].IntVal
						fileTotalErrors += doc.HeaderFields["errors"].IntVal
					} else {
						slog.Debug("\tflushToFilesAsync[%d], errors:", fi)
					}
				}

				for di := 0; di < int(state.Conf.ThreadsDbUpload); di++ {
					doc, ok := <-state.AsyncFlushToDbChannels[di]
					if ok && len(doc.HeaderFields) > 0 {
						slog.Debug("\tflushToDbAsync[%d], count:%d, errors:%d", di, doc.HeaderFields["count"].IntVal, doc.HeaderFields["errors"].IntVal)
						dbTotalCount += doc.HeaderFields["count"].IntVal
						dbTotalErrors += doc.HeaderFields["errors"].IntVal
					} else {
						slog.Debug("\tflushToDbAsync[%d], errors:", di)
					}
				}
			*/
		}
	} else if state.Conf.RunMode == "CREATE_JSON_DOC_ARCHIVE" {
		// home, _ := os.UserHomeDir()
		err := metLineTypeParser.WriteJsonToCompressedFile(state.CbDocs, state.Conf.JsonArchiveFilePathAndPrefix+time.Now().Format(time.RFC3339))
		if err != nil {
			slog.Error("Expected no error, got:", slog.Any("error", err))
		}
		return err
	}

	slog.Info("Run stats", "files", len(inputFiles), "docs", len(state.CbDocs), "fileTotalCount", fileTotalCount,
		"fileTotalErrors", fileTotalErrors, "dbTotalCount", dbTotalCount, "dbTotalErrors", dbTotalErrors,
		"run-time(ms)", time.Since(start).Milliseconds())
	slog.Info("Run stats", "Line Type Stats", state.LineTypeStats)
	return nil
}

func ParseConfig(file string) (types.ConfigJSON, error) {
	slog.Debug("parseConfig(" + file + ")")

	conf := types.ConfigJSON{}
	configFile, err := os.Open(file)
	if err != nil {
		slog.Error("opening config file:" + err.Error())
		configFile.Close()
		return conf, err
	}
	defer configFile.Close()

	jsonParser := json.NewDecoder(configFile)
	if err = jsonParser.Decode(&conf); err != nil {
		slog.Error("parsing config file:" + err.Error())
		return conf, err
	}

	return conf, nil
}

func GetCredentials(credentialsFilePath string) types.Credentials {
	creds := types.Credentials{}
	yamlFile, err := os.ReadFile(credentialsFilePath)
	if err != nil {
		slog.Debug("yamlFile.Get err:" + err.Error())
	}
	err = yaml.Unmarshal(yamlFile, &creds)
	if err != nil {
		slog.Error("Unmarshal:" + err.Error())
	}
	return creds
}

func ParseLoadSpec(file string) (types.LoadSpec, error) {
	slog.Debug("parseLoadSpec(" + file + ")")

	ls := types.LoadSpec{}
	configFile, err := os.Open(file)
	if err != nil {
		slog.Error("opening load_spec file:" + err.Error())
		configFile.Close()
		return ls, err
	}
	defer configFile.Close()

	jsonParser := json.NewDecoder(configFile)
	if err = jsonParser.Decode(&ls); err != nil {
		slog.Error("parsing load_spec file:" + err.Error())
		return ls, err
	}

	return ls, nil
}
