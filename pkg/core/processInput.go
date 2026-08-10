package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/dtcenter/METjson2db/pkg/async"
	"github.com/dtcenter/METjson2db/pkg/state"
	"github.com/dtcenter/METjson2db/pkg/storage"
	"github.com/dtcenter/METjson2db/pkg/types"
	"github.com/dtcenter/METstat2json/pkg/parser"
	"gopkg.in/yaml.v3"
)

// init runs before main() is evaluated
func init() {
	slog.Debug("ProcessInput:init()")
}

// ProcessInputFiles processes stat files from a list of paths (backward-compatible).
func ProcessInputFiles(inputFiles []string, preDbLoadCallback func()) error {
	provider := storage.NewLocalProvider(inputFiles)
	return ProcessFromProvider(context.Background(), provider, preDbLoadCallback)
}

// ProcessFromProvider processes stat files from a StorageProvider.
func ProcessFromProvider(ctx context.Context, provider storage.StorageProvider, preDbLoadCallback func()) error {
	slog.Info("ProcessFromProvider")

	start := time.Now()
	state.StateReset()

	if state.LoadSpec.RunMode == "DIRECT_LOAD_TO_DB" {
		if !state.LoadSpec.RunNonThreaded {
			for workerIdx := 0; workerIdx < int(state.LoadSpec.ThreadsDbUpload); workerIdx++ {

				state.AsyncFlushToDbChannels = append(state.AsyncFlushToDbChannels, make(chan map[string]interface{}, state.LoadSpec.ChannelBufferSizeNumberOfDocs))
				state.AsyncWaitGroupFlushToDb.Add(1)
				go func(workerID int) {
					defer state.AsyncWaitGroupFlushToDb.Done()
					async.FlushToDbAsync(ctx, workerID)
				}(workerIdx)
			}

			if !state.LoadSpec.OverWriteData {
				for workerIdx := 0; workerIdx < int(state.LoadSpec.ThreadsMergeDocFetch); workerIdx++ {

					state.AsyncMergeDocFetchChannels = append(state.AsyncMergeDocFetchChannels, make(chan string, state.LoadSpec.ChannelBufferSizeNumberOfDocs))
					state.AsyncWaitGroupMergeDocFetch.Add(1)
					go func(workerID int) {
						defer state.AsyncWaitGroupMergeDocFetch.Done()
						async.MergeDbDocFetchAsync(ctx, workerID)
					}(workerIdx)
				}
			}
		}
	}

	// Return any errors instead of continuing so the caller can correctly handle the error
	// e.g. - by deciding to not mark the queue message as resolved
	if err := startProcessingFromProvider(ctx, provider); err != nil {
		return fmt.Errorf("processing files: %w", err)
	}

	fileTotalCount := int64(0)
	fileTotalErrors := int64(0)
	dbTotalCount := int64(0)
	dbTotalErrors := int64(0)

	switch state.LoadSpec.RunMode {
	case "DIRECT_LOAD_TO_DB":
		if !state.LoadSpec.OverWriteData {
			for fi := 0; fi < int(state.LoadSpec.ThreadsMergeDocFetch); fi++ {
				state.AsyncMergeDocFetchChannels[fi] <- "endMarker"
			}
			state.AsyncWaitGroupMergeDocFetch.Wait()
			slog.Info("AsyncWaitGroupMergeDocFetch finished!")
		}

		if preDbLoadCallback != nil {
			preDbLoadCallback()
		}
		StatToCbFlush(true)
		if !state.LoadSpec.RunNonThreaded {
			slog.Debug("Waiting for threads to finish ...")

			// send end-marker doc to all channels
			endMarkerDoc := make(map[string]interface{})
			endMarkerDoc["endMarker"] = "endMarker"

			for workerIdx := 0; workerIdx < int(state.LoadSpec.ThreadsDbUpload); workerIdx++ {
				state.AsyncFlushToDbChannels[workerIdx] <- endMarkerDoc
			}

			state.AsyncWaitGroupFlushToDb.Wait()
			slog.Debug("asyncWaitGroupFlushToDb finished!")
		}
	case "CREATE_JSON_DOC_ARCHIVE":
		outputFilename := state.LoadSpec.JsonArchiveFilePathAndPrefix + time.Now().Format(time.RFC3339) + ".json.gz"
		err := parser.WriteJsonToCompressedFile(state.CbDocs, outputFilename)
		if err != nil {
			slog.Error("Expected no error, got:", slog.Any("error", err))
		}
		return err
	}

	slog.Info("Run stats", "docs", len(state.CbDocs), "fileTotalCount", fileTotalCount,
		"fileTotalErrors", fileTotalErrors, "dbTotalCount", dbTotalCount, "dbTotalErrors", dbTotalErrors,
		"run-time(ms)", time.Since(start).Milliseconds())
	slog.Info("Run stats", "Line Type Stats", state.LineTypeStats)
	return nil
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
