package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/NOAA-GSL/METdatacb/pkg/async"
	"github.com/NOAA-GSL/METdatacb/pkg/core"
	"github.com/NOAA-GSL/METdatacb/pkg/state"
	"github.com/NOAA-GSL/METdatacb/pkg/types"
	"github.com/NOAA-GSL/METdatacb/pkg/utils"
)

func main() {
	// Uncomment following line to enable logging
	// gocb.SetLogger(gocb.VerboseStdioLogger())

	slog.Info("METdatacb:main()")

	start := time.Now()

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

	flag.Parse()

	loadSpec, err := parseLoadSpec(loadSpecFilePath)
	if err != nil {
		log.Fatal("Unable to parse config")
		return
	}
	// fmt.Println("LoadSpec:\n" + utils.JsonPrettyPrintStruct(loadSpec))

	if len(inputFile) > 0 {
		log.Println("meta-update, settings file:" + settingsFilePath + ",credentials file:" + credentialsFilePath + ",inputFile:" + inputFile)
		inputFiles = append(inputFiles, inputFile)
	} else if len(inputFolder) > 0 {
		log.Println("meta-update, settings file:" + settingsFilePath + ",credentials file:" + credentialsFilePath + ",inputFolder:" + inputFolder)
		// add all files in folder
		files, err := os.ReadDir(inputFolder)
		if err != nil {
			log.Fatal(err)
		}
		for _, file := range files {
			if !file.IsDir() {
				inputFiles = append(inputFiles, inputFolder+file.Name())
			}
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
				log.Printf("Error opening folder:%s", folders[i])
				log.Fatal(err)
			}
			for _, file := range files {
				if !file.IsDir() {
					inputFiles = append(inputFiles, folders[i]+"/"+file.Name())
				}
			}
		}

	}

	if len(inputFiles) == 0 {
		log.Fatal("Must specify either an input file (-f), input folder (-i) OR must have load_spec files!")
		os.Exit(1)
	}

	state.Conf, err = parseConfig(settingsFilePath)
	if err != nil {
		log.Fatal("Unable to parse config")
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
		log.Fatal("No troubleshoot.json found, skipping trouble shooting ...")
		return
	}

	err = loadTypeTypeDefs()
	if err != nil {
		log.Fatal("Error loading Line Type definitions!", err)
		return
	}

	slog.Debug("Conf", "MaxFilesInProcessChunk", state.Conf.MaxFilesInProcessChunk)
	slog.Debug("Conf", "maxLinesToLoad", state.Conf.MaxLinesToLoad)
	slog.Debug("Conf", "flushToDbDataSectionMaxCount", state.Conf.FlushToDbDataSectionMaxCount)
	slog.Debug("Conf", "overWriteData", state.Conf.OverWriteData)
	slog.Debug("Conf", "writeJSONsToFile", state.Conf.WriteJSONsToFile)
	slog.Debug("Conf", "HeaderColumns length", len(state.Conf.HeaderColumns))
	slog.Debug("Conf", "CommonColumns length", len(state.Conf.CommonColumns))
	slog.Debug("Conf", "LineTypeColumns length", len(state.Conf.LineTypeColumns))

	state.Credentials = getCredentials(credentialsFilePath)
	if len(loadSpec.TargetCollection) > 0 {
		log.Printf("Using load_spec target collection:%s", loadSpec.TargetCollection)
		state.Credentials.Cb_collection = loadSpec.TargetCollection
	}
	log.Printf("DB:(%s.%s.%s)", state.Credentials.Cb_bucket, state.Credentials.Cb_scope, state.Credentials.Cb_collection)

	slog.Debug("inputFiles:\n%v", utils.PrettyPrint(inputFiles))
	log.Printf("inputFiles:%d", len(inputFiles))
	// log.Fatal("Exit hard coded in main.go:190")

	if !state.Conf.RunNonThreaded {
		for fi := 0; fi < int(state.Conf.ThreadsFileProcessor); fi++ {
			fi := fi
			state.AsyncFileProcessorChannels = append(state.AsyncFileProcessorChannels, make(chan string, state.Conf.ChannelBufferSizeNumberOfFiles))
			state.AsyncWaitGroupFileProcessor.Add(1)
			go func() {
				defer state.AsyncWaitGroupFileProcessor.Done()
				async.FileProcessorAsync(fi)
			}()
		}

		for fi := 0; fi < int(state.Conf.ThreadsWriteToDisk); fi++ {
			fi := fi
			state.AsyncFlushToFileChannels = append(state.AsyncFlushToFileChannels, make(chan types.CbDataDocument, state.Conf.ChannelBufferSizeNumberOfDocs))
			state.AsyncWaitGroupFlushToFiles.Add(1)
			go func() {
				defer state.AsyncWaitGroupFlushToFiles.Done()
				async.FlushToFilesAsync(fi)
			}()
		}

		for di := 0; di < int(state.Conf.ThreadsDbUpload); di++ {
			di := di
			state.AsyncFlushToDbChannels = append(state.AsyncFlushToDbChannels, make(chan types.CbDataDocument, state.Conf.ChannelBufferSizeNumberOfDocs))
			state.AsyncWaitGroupFlushToDb.Add(1)
			go func() {
				defer state.AsyncWaitGroupFlushToDb.Done()
				// conn := getDbConnection(credentials)
				async.FlushToDbAsync(di)
			}()
		}
	}

	// log.Fatal("Test exit!")

	core.StartProcessing(inputFiles)

	fileTotalCount := int64(0)
	fileTotalErrors := int64(0)
	dbTotalCount := int64(0)
	dbTotalErrors := int64(0)

	core.StatToCbFlush(true)
	if !state.Conf.RunNonThreaded {
		log.Printf("Waiting for threads to finish ...")

		// send end-marker doc to all channels
		endMarkerDoc := types.CbDataDocument{}

		for fi := 0; fi < int(state.Conf.ThreadsWriteToDisk); fi++ {
			state.AsyncFlushToFileChannels[fi] <- endMarkerDoc
		}

		for di := 0; di < int(state.Conf.ThreadsDbUpload); di++ {
			state.AsyncFlushToDbChannels[di] <- endMarkerDoc
		}

		state.AsyncWaitGroupFlushToFiles.Wait()
		log.Printf("asyncWaitGroupFlushToFiles finished!")
		state.AsyncWaitGroupFlushToDb.Wait()
		log.Printf("asyncWaitGroupFlushToDb finished!")

		for fi := 0; fi < int(state.Conf.ThreadsFileProcessor); fi++ {
			state.AsyncFileProcessorChannels[fi] <- "end"
		}
		state.AsyncWaitGroupFileProcessor.Wait()
		log.Printf("asyncWaitGroupFileProcessor finished!")

		// get return info from threads
		/*
			for fi := 0; fi < int(state.Conf.ThreadsWriteToDisk); fi++ {
				doc, ok := <-state.AsyncFlushToFileChannels[fi]
				if ok && len(doc.HeaderFields) > 0 {
					log.Printf("\tflushToFilesAsync[%d], count:%d, errors:%d", fi, doc.HeaderFields["count"].IntVal, doc.HeaderFields["errors"].IntVal)
					fileTotalCount += doc.HeaderFields["count"].IntVal
					fileTotalErrors += doc.HeaderFields["errors"].IntVal
				} else {
					log.Printf("\tflushToFilesAsync[%d], errors:", fi)
				}
			}

			for di := 0; di < int(state.Conf.ThreadsDbUpload); di++ {
				doc, ok := <-state.AsyncFlushToDbChannels[di]
				if ok && len(doc.HeaderFields) > 0 {
					log.Printf("\tflushToDbAsync[%d], count:%d, errors:%d", di, doc.HeaderFields["count"].IntVal, doc.HeaderFields["errors"].IntVal)
					dbTotalCount += doc.HeaderFields["count"].IntVal
					dbTotalErrors += doc.HeaderFields["errors"].IntVal
				} else {
					log.Printf("\tflushToDbAsync[%d], errors:", di)
				}
			}
		*/
	}

	slog.Info("Run stats", "files", len(inputFiles), "docs", len(state.CbDocs), "fileTotalCount", fileTotalCount,
		"fileTotalErrors", fileTotalErrors, "dbTotalCount", dbTotalCount, "dbTotalErrors", dbTotalErrors,
		"run-time(ms)", time.Since(start).Milliseconds())
	slog.Info("Run stats", "Line Type Stats", state.LineTypeStats)
}

func loadTypeTypeDefs() error {
	files, err := os.ReadDir(state.Conf.LineTypeDefs)
	if err != nil {
		log.Printf("Error loading Line Type definitions from:%s", state.Conf.LineTypeDefs)
		return err
	}
	for _, file := range files {
		if !file.IsDir() {
			ltFilePath := state.Conf.LineTypeDefs + "/" + file.Name()
			// log.Printf("LineType:%s", )

			deflt := types.DefLineType{}
			ltFile, err := os.Open(ltFilePath)
			if err != nil {
				log.Printf("Error opening Line Type definition file:%s,%v", ltFilePath, err.Error())
				ltFile.Close()
				return err
			}
			defer ltFile.Close()

			jsonParser := json.NewDecoder(ltFile)
			if err = jsonParser.Decode(&deflt); err != nil {
				log.Printf("Error parsing Line Type definition file:%s,%v", ltFilePath, err.Error())
				return err
			}

			lt := types.LineType{}

			lt.LineType = deflt.LineType
			for i := 0; i < len(deflt.Columns); i++ {
				stnames := deflt.Columns[i].Names
				sttype := deflt.Columns[i].Type
				for j := 0; j < len(stnames); j++ {
					lt.Columns = append(lt.Columns, types.Column{Name: stnames[j], Type: sttype})
				}
			}
			state.Conf.LineTypeColumns = append(state.Conf.LineTypeColumns, lt)
			if state.TroubleShoot.EnableLineTypeTrack {
				if slices.Contains(state.TroubleShoot.LineTypeTrack.LineTypeList, lt.LineType) {
					if slices.Contains(state.TroubleShoot.LineTypeTrack.Actions, "printLineTypeStatFileColumnMappingAndTerminate") {
						log.Printf(">>>>>>>>>>>>> Tracking[LineTypeTrack]:printLineTypeStatFileColumnMappingAndTerminate:LineType:%s\n", lt.LineType)
						for ti := 0; ti < len(lt.Columns); ti++ {
							log.Printf("%d\t%s\t%s",
								len(state.Conf.CommonColumns)+ti+1,
								lt.Columns[ti].Name, lt.Columns[ti].Type)
						}
						log.Fatal("Terminating after track...")
					}
				}
			}
		}
	}
	return err
}

func parseLoadSpec(file string) (types.LoadSpec, error) {
	log.Println("parseLoadSpec(" + file + ")")

	ls := types.LoadSpec{}
	configFile, err := os.Open(file)
	if err != nil {
		log.Fatal("opening load_spec file", err.Error())
		configFile.Close()
		return ls, err
	}
	defer configFile.Close()

	jsonParser := json.NewDecoder(configFile)
	if err = jsonParser.Decode(&ls); err != nil {
		log.Fatalln("parsing load_spec file", err.Error())
		return ls, err
	}

	return ls, nil
}

func parseConfig(file string) (types.ConfigJSON, error) {
	log.Println("parseConfig(" + file + ")")

	conf := types.ConfigJSON{}
	configFile, err := os.Open(file)
	if err != nil {
		log.Fatal("opening config file", err.Error())
		configFile.Close()
		return conf, err
	}
	defer configFile.Close()

	jsonParser := json.NewDecoder(configFile)
	if err = jsonParser.Decode(&conf); err != nil {
		log.Fatalln("parsing config file", err.Error())
		return conf, err
	}

	return conf, nil
}

func parseTroubleShoot(file string) (types.TroubleShoot, error) {
	log.Println("parseTroubleShoot(" + file + ")")

	ts := types.TroubleShoot{}
	tsFile, err := os.Open(file)
	if err != nil {
		log.Printf("opening troubleshoot.json file:%s", err.Error())
		tsFile.Close()
		return ts, err
	}
	defer tsFile.Close()

	jsonParser := json.NewDecoder(tsFile)
	if err = jsonParser.Decode(&ts); err != nil {
		log.Fatalln("parsing troubleshoot.json file", err.Error())
		return ts, err
	}

	return ts, nil
}

func getCredentials(credentialsFilePath string) types.Credentials {
	creds := types.Credentials{}
	yamlFile, err := os.ReadFile(credentialsFilePath)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, &creds)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
	return creds
}
