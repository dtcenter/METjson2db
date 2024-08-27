package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
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
		fields := strings.Fields(lineStr)
		fmt.Println(lineCount, ":", fields, len(fields))
		// _ = fields // remove declared but not used errors
		lineType := fields[23]
		statFieldsToCbDoc(lineType, fields)

		// check if time to flush cbDocs to files and/or db
		// if so also init cbDocs after that
		/*
			builder := getBuilder(lineType, cbLineTypeColDefs[lineType], fields)
			if nil != builder {
				builder.processFields()
			} else {
				fmt.Println("Unknown line tye:", lineType)
			}
		*/
	}
	fmt.Println("lineCount:", lineCount)

	return nil
}

func statFieldsToCbDoc(lineType string, fields []string) {
	log.Println("statFieldsToCbDoc(" + lineType + ")")

	coldef, ok := cbLineTypeColDefs[lineType]
	if !ok {
		fmt.Println("no coldef for lineType:", lineType)
		return
	}
	fmt.Println("fields[]:", len(fields), ",coldef[]:", len(coldef))

	id := ""
	for i := 0; i < len(coldef); i++ {
		if coldef[i].IsID {
			id = id + ":" + fields[i]
		}
	}
	doc, ok := cbDocs[id]
	if !ok {
		doc = CbDataDocument{}
		doc.init()
		cbDocs[id] = doc
		doc.headerFields["id"] = makeStringCbDataValue(id)

		// need to populate header fields
		for i := 0; i < len(coldef); i++ {
			if coldef[i].IsHeader {
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

	// now append data fields to doc
	for i := 0; i < len(coldef); i++ {
		if !coldef[i].IsHeader {
			dsec := DataSection{}
			fmt.Println("data key:", fields[dataKeyIdx])
			doc.data[fields[dataKeyIdx]] = dsec
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

	fmt.Println("Cb doc:\n", doc.toJSONString())
}
