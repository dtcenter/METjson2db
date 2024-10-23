package main

import (
	"bufio"
	//	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
)

// init runs before main() is evaluated
func init() {
	log.Println("statToJSON:init()")
}

func statFileToCbDoc(filepath string) error {
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

	for fileScanner.Scan() {
		lineStr := fileScanner.Text()
		lineCount += 1
		if lineCount == 1 {
			continue
		}
		fields := strings.Fields(lineStr)
		// log.Printf("%d:%v:%d", lineCount, fields, len(fields))
		// _ = fields // remove declared but not used errors
		lineType := fields[23]
		doc := statFieldsToCbDoc(lineType, fields)

		if len(doc.headerFields) < 2 || len(doc.data) == 0 {
			log.Printf("NULL document[%s]", doc.headerFields["ID"].StringVal)
			log.Printf("lineType:%s, filepath:%s, line:%d", lineType, filepath, lineCount)
			continue
		}

		/* check if time to flush cbDocs to files and/or db
		See spec in readme, section:
		# Output location, configuration and logic
		*/
		totalLinesProcessed++
		if (totalLinesProcessed % 1000) == 0 {
			statToCbFlush(false)
		}

		// if so also init cbDocs after that
		/*
			builder := getBuilder(lineType, cbLineTypeColDefs[lineType], fields)
			if nil != builder {
				builder.processFields()
			} else {
				log.Printf("Unknown line tye:", lineType)
			}
		*/

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

func statFieldsToCbDoc(lineType string, fields []string) CbDataDocument {
	// log.Println("statFieldsToCbDoc(" + lineType + ")")

	coldef, ok := cbLineTypeColDefs[lineType]
	if !ok {
		log.Printf("no coldef for lineType:%s", lineType)
		lineTypeStats[lineType] = LineTypeStat{0, false}
		return CbDataDocument{}
	}
	lt, ok := lineTypeStats[lineType]
	if !ok {
		lineTypeStats[lineType] = LineTypeStat{1, true}
	} else {
		lineTypeStats[lineType] = LineTypeStat{lt.ProcessedCount + 1, true}
	}
	// log.Printf("fields[]:%d, coldef[]:%d", len(fields), len(coldef))

	id := ""
	for i := 0; i < len(coldef); i++ {
		if coldef[i].IsID {
			id = id + ":" + fields[i]
		}
	}

	cbDocsMutex.RLock()
	doc, ok := cbDocs[id]
	cbDocsMutex.RUnlock()

	if !ok {
		doc = CbDataDocument{}
		doc.init()
		cbDocsMutex.Lock()
		cbDocs[id] = doc
		cbDocsMutex.Unlock()
		doc.headerFields["ID"] = makeStringCbDataValue(id)

		// need to populate header fields
		for i := 0; i < len(coldef); i++ {
			if coldef[i].IsHeader && !slices.Contains(conf.IgnoreColumns, coldef[i].Name) && !slices.Contains(conf.IgnoreValues, fields[i]) {
				switch coldef[i].DataType {
				case 0:
					doc.headerFields[coldef[i].Name] = makeStringCbDataValue(fields[i])
				case 1:
					intv, _ := strconv.Atoi(fields[i])
					doc.headerFields[coldef[i].Name] = makeIntCbDataValue(int64(intv))
				case 2:
					floatv, _ := strconv.ParseFloat(fields[i], 64)
					doc.headerFields[coldef[i].Name] = makeFloatCbDataValue(floatv)
				case 3:
					doc.headerFields[coldef[i].Name] = makeIntCbDataValue(statDateToEpoh(fields[i]))
				}
			}
		}

	}

	doc.mutex.Lock()
	doc.flushed = false
	// now append data fields to doc
	dsec := DataSection{}
	// log.Printf("data key:%s", fields[dataKeyIdx])
	// log.Printf("fields:\n", fields)
	dataKey := fields[dataKeyIdx]
	doc.data[dataKey] = dsec
	for i := 0; i < len(coldef); i++ {
		if !coldef[i].IsHeader && !slices.Contains(conf.DataKeyColumns, coldef[i].Name) && !slices.Contains(conf.IgnoreColumns, coldef[i].Name) && !slices.Contains(conf.IgnoreValues, fields[i]) {
			switch coldef[i].DataType {
			case 0:
				dsec[coldef[i].Name] = makeStringCbDataValue(fields[i])
			case 1:
				intv, _ := strconv.Atoi(fields[i])
				dsec[coldef[i].Name] = makeIntCbDataValue(int64(intv))
			case 2:
				floatv, _ := strconv.ParseFloat(fields[i], 64)
				dsec[coldef[i].Name] = makeFloatCbDataValue(floatv)
			case 3:
				dsec[coldef[i].Name] = makeIntCbDataValue(statDateToEpoh(fields[i]))
			}
		}
	}

	doc.mutex.Unlock()
	return doc
}
