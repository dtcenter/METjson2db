package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
	// "github.com/couchbase/gocb/v2"

	"github.com/NOAA-GSL/METdatacb/statToCbDoc/pkg/async"
	"github.com/NOAA-GSL/METdatacb/statToCbDoc/pkg/common"
	"github.com/NOAA-GSL/METdatacb/statToCbDoc/pkg/core"
	"github.com/NOAA-GSL/METdatacb/statToCbDoc/pkg/state"
	"github.com/NOAA-GSL/METdatacb/statToCbDoc/pkg/types"
	"github.com/NOAA-GSL/METdatacb/statToCbDoc/pkg/utils"
)

func main() {
	// Uncomment following line to enable logging
	// gocb.SetLogger(gocb.VerboseStdioLogger())

	start := time.Now()
	log.Print("meta-update:main()")

	common.cbLineTypeColDefs = make(map[string]types.ColDefArray)
	common.cbDocs = make(map[string]types.CbDataDocument)
	state.cbDocsMutex = &sync.RWMutex{}
	state.docKeyCountMapMutex = &sync.RWMutex{}
	state.docKeyCountMap = make(map[string]types.DocKeyCounts)

	state.lineTypeStats = make(map[string]types.LineTypeStat)

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
	inputFiles = append(inputFiles, inputFile)

	var inputFolder string
	flag.StringVar(&inputFolder, "i", "", "input stat files folder")

	flag.Parse()

	loadSpec, err := parseLoadSpec(loadSpecFilePath)
	if err != nil {
		log.Fatal("Unable to parse config")
		return
	}
	log.Printf("folder_tmpl:%d", len(loadSpec.FolderTmpl))
	log.Printf("LoadVal.Field[0].Val length:%d", len(loadSpec.LoadVal.Field[0].Val))
	fmt.Println("LoadSpec:\n" + utils.jsonPrettyPrintStruct(loadSpec))

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

		for vi := 0; vi < len(loadSpec.LoadVal.Field[0].Val); vi++ {
			fname := "{" + loadSpec.LoadVal.Field[0].Name + "}"
			folders = append(folders, strings.Replace(loadSpec.FolderTmpl, fname, loadSpec.LoadVal.Field[0].Val[vi], -1))
		}

		for fi := 1; fi < len(loadSpec.LoadVal.Field); fi++ {
			fname := "{" + loadSpec.LoadVal.Field[fi].Name + "}"
			for vi := 0; vi < len(loadSpec.LoadVal.Field[fi].Val); vi++ {
				for i := 0; i < len(folders); i++ {
					folders[i] = strings.Replace(folders[i], fname, loadSpec.LoadVal.Field[fi].Val[vi], -1)
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

	state.conf, err = parseConfig(settingsFilePath)
	if err != nil {
		log.Fatal("Unable to parse config")
		return
	}

	state.troubleShoot, err = parseTroubleShoot("troubleshoot.json")
	if err != nil {
		log.Fatal("No troubleshoot.json found, skipping trouble shooting ...")
		return
	}

	log.Printf("MaxFilesInProcessChunk:%d", state.conf.MaxFilesInProcessChunk)
	log.Printf("maxLinesToLoad:%d", state.conf.MaxLinesToLoad)
	log.Printf("flushToDbDataSectionMaxCount:%d", state.conf.FlushToDbDataSectionMaxCount)
	log.Printf("overWriteData:%t", state.conf.OverWriteData)
	log.Printf("writeJSONsToFile:%t", state.conf.WriteJSONsToFile)
	log.Printf("HeaderColumns length:%d", len(state.conf.HeaderColumns))
	log.Printf("CommonColumns length:%d", len(state.conf.CommonColumns))
	log.Printf("LineTypeColumns length:%d", len(state.conf.LineTypeColumns))

	state.credentials = getCredentials(credentialsFilePath)
	if len(loadSpec.TargetCollection) > 0 {
		log.Printf("Using load_spec target collection:%s", loadSpec.TargetCollection)
		state.credentials.Cb_collection = loadSpec.TargetCollection
	}
	log.Printf("DB:(%s.%s.%s)", state.credentials.Cb_bucket, state.credentials.Cb_scope, state.credentials.Cb_collection)

	generateColDefsFromConfig(state.conf, state.cbLineTypeColDefs)

	// log.Printf("inputFiles:\n%v", inputFiles)
	log.Printf("inputFiles:%d", len(inputFiles))

	if !state.conf.RunNonThreaded {
		for fi := 0; fi < int(state.conf.ThreadsFileProcessor); fi++ {
			fi := fi
			state.asyncFileProcessorChannels = append(state.asyncFileProcessorChannels, make(chan string, state.conf.ChannelBufferSizeNumberOfFiles))
			state.asyncWaitGroupFileProcessor.Add(1)
			go func() {
				defer state.asyncWaitGroupFileProcessor.Done()
				async.fileProcessorAsync(fi)
			}()
		}

		for fi := 0; fi < int(conf.ThreadsWriteToDisk); fi++ {
			fi := fi
			asyncFlushToFileChannels = append(asyncFlushToFileChannels, make(chan CbDataDocument, conf.ChannelBufferSizeNumberOfDocs))
			asyncWaitGroupFlushToFiles.Add(1)
			go func() {
				defer asyncWaitGroupFlushToFiles.Done()
				flushToFilesAsync(fi)
			}()
		}

		for di := 0; di < int(conf.ThreadsDbUpload); di++ {
			di := di
			asyncFlushToDbChannels = append(asyncFlushToDbChannels, make(chan CbDataDocument, conf.ChannelBufferSizeNumberOfDocs))
			asyncWaitGroupFlushToDb.Add(1)
			go func() {
				defer asyncWaitGroupFlushToDb.Done()
				// conn := getDbConnection(credentials)
				flushToDbAsync(di)
			}()
		}
	}

	startProcessing(inputFiles)

	fileTotalCount := int64(0)
	fileTotalErrors := int64(0)
	dbTotalCount := int64(0)
	dbTotalErrors := int64(0)

	if !conf.RunNonThreaded {
		log.Printf("Waiting for threads to finish ...")

		// send end-marker doc to all channels
		endMarkerDoc := CbDataDocument{}
		endMarkerDoc.init()

		for fi := 0; fi < int(conf.ThreadsWriteToDisk); fi++ {
			asyncFlushToFileChannels[fi] <- endMarkerDoc
		}

		for di := 0; di < int(conf.ThreadsDbUpload); di++ {
			asyncFlushToDbChannels[di] <- endMarkerDoc
		}

		asyncWaitGroupFlushToFiles.Wait()
		log.Printf("asyncWaitGroupFlushToFiles finished!")
		asyncWaitGroupFlushToDb.Wait()
		log.Printf("asyncWaitGroupFlushToDb finished!")

		for fi := 0; fi < int(conf.ThreadsFileProcessor); fi++ {
			asyncFileProcessorChannels[fi] <- "end"
		}
		asyncWaitGroupFileProcessor.Wait()
		log.Printf("asyncWaitGroupFileProcessor finished!")

		// get return info from threads
		for fi := 0; fi < int(conf.ThreadsWriteToDisk); fi++ {
			doc, ok := <-asyncFlushToFileChannels[fi]
			if ok && len(doc.headerFields) > 0 {
				log.Printf("\tflushToFilesAsync[%d], count:%d, errors:%d", fi, doc.headerFields["count"].IntVal, doc.headerFields["errors"].IntVal)
				fileTotalCount += doc.headerFields["count"].IntVal
				fileTotalErrors += doc.headerFields["errors"].IntVal
			} else {
				log.Printf("\tflushToFilesAsync[%d], errors:", fi)

			}
		}

		for di := 0; di < int(conf.ThreadsDbUpload); di++ {
			doc, ok := <-asyncFlushToDbChannels[di]
			if ok && len(doc.headerFields) > 0 {
				log.Printf("\tflushToDbAsync[%d], count:%d, errors:%d", di, doc.headerFields["count"].IntVal, doc.headerFields["errors"].IntVal)
				dbTotalCount += doc.headerFields["count"].IntVal
				dbTotalErrors += doc.headerFields["errors"].IntVal
			} else {
				log.Printf("\tflushToDbAsync[%d], errors:", di)
			}
		}
	}

	// conn := getDbConnection(credentials)
	// log.Printf("Connected to Couchbase:%s", conn.vxDBTARGET)

	/*
		cbDoc0, err := readCbDocument("/Users/gopa.padmanabhan/git/ascend/METdatacb/docs/MET_cb_doc_v1_epoch.json")
		if err == nil {
			log.Printf("Cb doc:\n", cbDoc0.toJSONString())
		}
	*/

	log.Printf("\tstatToCbDoc, files:%d, docs:%d, file-stats:[%d,%d], db-stats[%d,%d] finished in %v", len(inputFiles),
		len(cbDocs), fileTotalCount, fileTotalErrors, dbTotalCount, dbTotalErrors, time.Since(start))
	log.Printf("Line Type Stats:%v", jsonPrettyPrintStruct(lineTypeStats))
}

func parseLoadSpec(file string) (LoadSpec, error) {
	log.Println("parseLoadSpec(" + file + ")")

	ls := LoadSpec{}
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

func parseConfig(file string) (ConfigJSON, error) {
	log.Println("parseConfig(" + file + ")")

	conf := ConfigJSON{}
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

func parseTroubleShoot(file string) (TroubleShoot, error) {

	log.Println("parseTroubleShoot(" + file + ")")

	ts := TroubleShoot{}
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

func getCredentials(credentialsFilePath string) Credentials {
	creds := Credentials{}
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

func generateColDefsFromConfig(conf ConfigJSON, cbLineTypeColDefs map[string]ColDefArray) {
	for i := 0; i < len(conf.LineTypeColumns); i++ {
		lt := conf.LineTypeColumns[i].LineType
		ltcols := conf.LineTypeColumns[i].Columns
		doc := CbDataDocument{}
		doc.init()

		// first add common columns
		ccols := conf.CommonColumns
		cbLineTypeColDefs[lt] = make(ColDefArray, len(ccols)+len(ltcols))

		for cci := 0; cci < len(ccols); cci++ {
			ccol := ccols[cci]
			coldef := ColDef{}
			coldef.Name = ccol.Name
			switch {
			case ccol.Type == "string":
				coldef.DataType = 0
			case ccol.Type == "int":
				coldef.DataType = 1
			case ccol.Type == "float":
				coldef.DataType = 2
			case ccol.Type == "epoch":
				coldef.DataType = 3
			}
			coldef.IsID = slices.Contains(conf.IdColumns, coldef.Name)
			coldef.IsHeader = slices.Contains(conf.HeaderColumns, coldef.Name)
			coldef.IsDataKey = slices.Contains(conf.DataKeyColumns, coldef.Name)
			if coldef.IsDataKey {
				dataKeyIdx = cci
			}
			cbLineTypeColDefs[lt][cci] = coldef
		}

		// now add line type specific columns
		for ltci := 0; ltci < len(ltcols); ltci++ {
			ltcol := ltcols[ltci]
			coldef := ColDef{}
			coldef.Name = ltcol.Name
			switch {
			case ltcol.Type == "string":
				coldef.DataType = 0
			case ltcol.Type == "int":
				coldef.DataType = 1
			case ltcol.Type == "float":
				coldef.DataType = 2
			case ltcol.Type == "epoch":
				coldef.DataType = 3
			}
			cbLineTypeColDefs[lt][len(ccols)+ltci] = coldef
		}
		// log.Printf("ColDefs for:", lt, ":\n", cbLineTypeColDefs[lt])
	}
}
