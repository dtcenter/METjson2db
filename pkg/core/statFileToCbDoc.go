package core

import (
	"bufio"
	//	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/NOAA-GSL/METdatacb/pkg/state"
	"github.com/NOAA-GSL/METdatacb/pkg/types"
	"github.com/NOAA-GSL/METdatacb/pkg/utils"
)

// init runs before main() is evaluated
func init() {
	log.Println("statToJSON:init()")
}

func StatFileToCbDoc(filepath string) error {
	log.Println("statFileToCbDoc(" + filepath + ")")

	inputFile, err := os.Open(filepath)
	if err != nil {
		log.Fatal("error opening file\n", err.Error())
		inputFile.Close()
		return err
	}
	defer inputFile.Close()

	fileScanner := bufio.NewScanner(inputFile)
	fileScanner.Split(bufio.ScanLines)
	lineCount := 0
	trackLineTypeCount := 0

	for fileScanner.Scan() {
		lineStr := fileScanner.Text()
		lineCount += 1
		if lineCount == 1 {
			continue
		}
		fields := strings.Fields(lineStr)
		if len(fields) < 24 {
			continue
		}
		// log.Printf("%d:%v:%d", lineCount, fields, len(fields))
		// _ = fields // remove declared but not used errors
		lineType := fields[23]

		coldef, ok := state.CbLineTypeColDefs[lineType]
		if !ok {
			if state.TroubleShoot.EnableLineTypeTrack {
				if slices.Contains(state.TroubleShoot.LineTypeTrack.Actions, "printUnhandledLineTypesAndDataLines") {
					log.Printf("no coldef for lineType:%s, file:%s, line#:%d", lineType, filepath, lineCount)
					log.Printf("%s", lineStr)
				}
			}
			state.LineTypeStats[lineType] = types.LineTypeStat{ProcessedCount: 0, Handled: false}
			continue
		}
		if len(coldef) > len(fields) {
			log.Printf("Column definition len(coldef) > len(data)!, file:%s, linetype:%s, line:%d, len(coldef):%d, len(fields):%d",
				filepath, lineType, lineCount, len(coldef), len(fields))
			continue
		}

		doc := statFieldsToCbDoc(lineType, fields, coldef)

		if len(doc.HeaderFields) < 2 || len(doc.Data) == 0 {
			// log.Printf("NULL document[%s]", doc.HeaderFields["ID"].StringVal)
			// log.Printf("lineType:%s, filepath:%s, line:%d", lineType, filepath, lineCount)
			continue
		}

		if state.TroubleShoot.EnableLineTypeTrack {
			if slices.Contains(state.TroubleShoot.LineTypeTrack.LineTypeList, lineType) {
				if slices.Contains(state.TroubleShoot.LineTypeTrack.Actions, "printSampleStatFileDataLinesAndTerminate") {
					trackLineTypeCount++
					log.Printf(">>>>>>>>>>>>> Tracking[LineTypeTrack]:%s stat-file-data-line:\n%s\n", lineType, lineStr)
					if trackLineTypeCount > 10 {
						log.Fatal("Exiting after track ....")
					}
				}
			}
		}

		/* check if time to flush cbDocs to files and/or db
		See spec in readme, section:
		# Output location, configuration and logic
		*/
		state.TotalLinesProcessed++
		if (state.TotalLinesProcessed % 1000) == 0 {
			statToCbFlush(false)
		}

		/*
			if totalLinesProcessed == 100 {
				break
			}
		*/

	}
	statToCbFlush(true)
	log.Printf("lineCount:%d", lineCount)

	return nil
}

func statFieldsToCbDoc(lineType string, fields []string, coldef types.ColDefArray) types.CbDataDocument {
	// log.Println("statFieldsToCbDoc(" + lineType + ")")

	lt, ok := state.LineTypeStats[lineType]
	if !ok {
		state.LineTypeStats[lineType] = types.LineTypeStat{ProcessedCount: 1, Handled: true}
	} else {
		state.LineTypeStats[lineType] = types.LineTypeStat{ProcessedCount: (lt.ProcessedCount + 1), Handled: true}
	}
	// log.Printf("fields[]:%d, coldef[]:%d", len(fields), len(coldef))

	id := ""
	for i := 0; i < len(coldef); i++ {
		if i > 0 {
			id = id + ":"
		}
		if coldef[i].IsID {
			if coldef[i].DataType == 3 {
				id = id + strconv.FormatInt(utils.StatDateToEpoh(fields[i]), 10)
			} else {
				id = id + strings.TrimSpace(fields[i])
			}
		}
	}
	if len(state.TroubleShoot.DocIdSizeTrack.Actions) > 0 {
		if len(id) > int(state.Conf.MaxDocIdLength) {
			log.Printf(">>>>>>>>>>>>> Tracking[DocIdSizeTrack], ID:%s len:%d", id, len(id))
		}
	}

	state.CbDocsMutex.RLock()
	doc, ok := state.CbDocs[id]
	state.CbDocsMutex.RUnlock()

	if !ok {
		doc = types.CbDataDocument{}
		doc.Init()
		state.CbDocsMutex.Lock()
		state.CbDocs[id] = doc
		state.CbDocsMutex.Unlock()
		doc.HeaderFields["ID"] = types.MakeStringCbDataValue(id)

		// need to populate header fields
		for i := 0; i < len(coldef); i++ {
			if coldef[i].IsHeader && !slices.Contains(state.Conf.IgnoreColumns, coldef[i].Name) && !slices.Contains(state.Conf.IgnoreValues, fields[i]) {
				switch coldef[i].DataType {
				case 0:
					doc.HeaderFields[coldef[i].Name] = types.MakeStringCbDataValue(fields[i])
				case 1:
					intv, err := strconv.Atoi(fields[i])
					if err == nil {
						doc.HeaderFields[coldef[i].Name] = types.MakeIntCbDataValue(int64(intv))
					}
				case 2:
					floatv, err := strconv.ParseFloat(fields[i], 64)
					if err == nil {
						doc.HeaderFields[coldef[i].Name] = types.MakeFloatCbDataValue(floatv)
					}
				case 3:
					doc.HeaderFields[coldef[i].Name] = types.MakeIntCbDataValue(utils.StatDateToEpoh(fields[i]))
				}
			}
		}

	}

	doc.Mutex.Lock()
	doc.Flushed = false
	// now append data fields to doc
	dsec := types.DataSection{}
	// log.Printf("data key:%s", fields[dataKeyIdx])
	// log.Printf("fields:\n", fields)
	dataKey := fields[state.DataKeyIdx]
	doc.Data[dataKey] = dsec
	for i := 0; i < len(coldef); i++ {
		if !coldef[i].IsHeader && !slices.Contains(state.Conf.DataKeyColumns, coldef[i].Name) &&
			!slices.Contains(state.Conf.IgnoreColumns, coldef[i].Name) && !slices.Contains(state.Conf.IgnoreValues, fields[i]) {
			switch coldef[i].DataType {
			case 0:
				dsec[coldef[i].Name] = types.MakeStringCbDataValue(fields[i])
			case 1:
				intv, _ := strconv.Atoi(fields[i])
				dsec[coldef[i].Name] = types.MakeIntCbDataValue(int64(intv))
			case 2:
				floatv, _ := strconv.ParseFloat(fields[i], 64)
				dsec[coldef[i].Name] = types.MakeFloatCbDataValue(floatv)
			case 3:
				dsec[coldef[i].Name] = types.MakeIntCbDataValue(utils.StatDateToEpoh(fields[i]))
			}
		}
	}

	doc.Mutex.Unlock()
	return doc
}
