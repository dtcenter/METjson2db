package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/NOAA-GSL/METdatacb/pkg/core"
	"github.com/NOAA-GSL/METdatacb/pkg/state"
	"github.com/NOAA-GSL/METdatacb/pkg/types"
)

func main() {
	// Uncomment following line to enable logging
	// gocb.SetLogger(gocb.VerboseStdioLogger())

	slog.Info("METdatacb:main()")

	home, _ := os.UserHomeDir()
	var credentialsFilePath string
	flag.StringVar(&credentialsFilePath, "c", home+"/credentials", "path to credentials file")

	var settingsFilePath string
	flag.StringVar(&settingsFilePath, "s", "./settings.json", "path to settings.json")

	var loadSpecFilePath string
	flag.StringVar(&loadSpecFilePath, "l", "./load_spec.json", "path to load_spec.json")

	var inputFile string
	flag.StringVar(&inputFile, "f", "", "stat file full path")
	var inputFiles []string

	var inputFolder string
	flag.StringVar(&inputFolder, "i", "", "input stat files folder")

	var inputFolderRecursive string
	flag.StringVar(&inputFolderRecursive, "I", "", "input stat files folder (recursive)")

	var fileNameRegEx string
	flag.StringVar(&fileNameRegEx, "r", "", "input filename match regex")

	flag.Parse()

	loadSpec, err := core.ParseLoadSpec(loadSpecFilePath)
	if err != nil {
		slog.Error("Unable to parse config")
		return
	}
	// fmt.Println("LoadSpec:\n" + utils.JsonPrettyPrintStruct(loadSpec))

	if len(inputFile) > 0 {
		slog.Debug("meta-update, settings file:" + settingsFilePath + ",credentials file:" + credentialsFilePath + ",inputFile:" + inputFile)
		inputFiles = append(inputFiles, inputFile)
	} else if len(inputFolder) > 0 {
		slog.Debug("meta-update, settings file:" + settingsFilePath + ",credentials file:" + credentialsFilePath + ",inputFolder:" + inputFolder)
		// add all files in folder
		files, err := os.ReadDir(inputFolder)
		if err != nil {
			slog.Error(fmt.Sprintf("%v", err))
		}
		for _, file := range files {
			if !file.IsDir() {
				inputFiles = append(inputFiles, inputFolder+file.Name())
			}
		}

	} else if len(inputFolderRecursive) > 0 {
		slog.Debug("meta-update, settings file:" + settingsFilePath + ",credentials file:" + credentialsFilePath + ",inputFolderRecursive:" + inputFolderRecursive)
		// add all files in folder
		regEx := "(.*?)"
		if len(fileNameRegEx) > 0 {
			regEx = fileNameRegEx
		}
		libRegEx, e := regexp.Compile(regEx)
		if e != nil {
			log.Fatal(e)
		}
		err = filepath.Walk(inputFolderRecursive, func(path string, info os.FileInfo, err error) error {
			if !info.IsDir() {
				if err == nil && libRegEx.MatchString(info.Name()) {
					inputFiles = append(inputFiles, path)
				}
			}
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	} else {
		// process load_spec, not a robust logic yet, but will work correctly if the template has all field markers, i.e. {name}s
		// and len(field[0].val) is the largest.

		folders := []string{}

		if !strings.Contains(loadSpec.FolderTmpl, "{") {
			folders = append(folders, loadSpec.FolderTmpl)
		} else {
			for vi := 0; vi < len(loadSpec.LoadVal.Field[0].Val); vi++ {
				fname := "{" + loadSpec.LoadVal.Field[0].Name + "}"
				if strings.Contains(loadSpec.FolderTmpl, fname) {
					folders = append(folders, strings.Replace(loadSpec.FolderTmpl, fname, loadSpec.LoadVal.Field[0].Val[vi], -1))
				}
			}

			for fi := 1; fi < len(loadSpec.LoadVal.Field); fi++ {
				fname := "{" + loadSpec.LoadVal.Field[fi].Name + "}"
				for vi := 0; vi < len(loadSpec.LoadVal.Field[fi].Val); vi++ {
					for i := 0; i < len(folders); i++ {
						folders[i] = strings.Replace(folders[i], fname, loadSpec.LoadVal.Field[fi].Val[vi], -1)
					}
				}
			}
		}
		fmt.Println(folders)

		for i := 0; i < len(folders); i++ {
			files, err := os.ReadDir(folders[i])
			if err != nil {
				slog.Debug("Error opening folder:" + folders[i])
				slog.Error("Error:", slog.Any("error", err))
			}
			for _, file := range files {
				if !file.IsDir() {
					inputFiles = append(inputFiles, folders[i]+"/"+file.Name())
				}
			}
		}

	}

	if len(inputFiles) == 0 {
		slog.Error("Must specify either an input file (-f), input folder (-i) OR must have load_spec files!")
		os.Exit(1)
	}

	state.Conf, err = core.ParseConfig(settingsFilePath)
	if err != nil {
		slog.Error("Unable to parse config")
		return
	}

	level := slog.LevelError

	switch state.Conf.LogLevel {
	case "DEBUG":
		level = slog.LevelDebug
	case "INFO":
		level = slog.LevelInfo
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	}
	opts := &slog.HandlerOptions{
		AddSource: true,
		Level:     level,
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
	slog.SetDefault(logger)

	state.TroubleShoot, err = parseTroubleShoot("troubleshoot.json")
	if err != nil {
		slog.Error("No troubleshoot.json found, skipping trouble shooting ...")
		return
	}

	slog.Debug("Conf", "MaxFilesInProcessChunk", state.Conf.MaxFilesInProcessChunk)
	slog.Debug("Conf", "maxLinesToLoad", state.Conf.MaxLinesToLoad)
	slog.Debug("Conf", "flushToDbDataSectionMaxCount", state.Conf.FlushToDbDataSectionMaxCount)
	slog.Debug("Conf", "overWriteData", state.Conf.OverWriteData)
	slog.Debug("Conf", "writeJSONsToFile", state.Conf.WriteJSONsToFile)
	slog.Debug("Conf", "HeaderColumns length", len(state.Conf.HeaderColumns))
	slog.Debug("Conf", "CommonColumns length", len(state.Conf.CommonColumns))

	state.Credentials = core.GetCredentials(credentialsFilePath)
	if len(loadSpec.TargetCollection) > 0 {
		slog.Debug("Using load_spec target collection:" + loadSpec.TargetCollection)
		state.Credentials.Cb_collection = loadSpec.TargetCollection
	}
	slog.Debug(fmt.Sprintf("DB:(%s.%s.%s)", state.Credentials.Cb_bucket, state.Credentials.Cb_scope, state.Credentials.Cb_collection))

	// slog.Debug("inputFiles:\n%v", utils.PrettyPrint(inputFiles))
	slog.Debug("inputFiles:", slog.Any("inputFiles", inputFiles))
	// slog.Error("Exit hard coded in main.go:190")

	err = core.ProcessInputFiles(inputFiles, postProcessDocsDefault)
	if err != nil {
		slog.Error("Error processing input files:" + err.Error())
	}
}

func postProcessDocsDefault() {

}

func parseTroubleShoot(file string) (types.TroubleShoot, error) {
	slog.Debug("parseTroubleShoot(" + file + ")")

	ts := types.TroubleShoot{}
	tsFile, err := os.Open(file)
	if err != nil {
		slog.Debug("opening troubleshoot.json file:" + err.Error())
		tsFile.Close()
		return ts, err
	}
	defer tsFile.Close()

	jsonParser := json.NewDecoder(tsFile)
	if err = jsonParser.Decode(&ts); err != nil {
		slog.Error("parsing troubleshoot.json file:" + err.Error())
		return ts, err
	}

	return ts, nil
}
