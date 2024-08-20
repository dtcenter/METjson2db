package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
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

	coldef := cbLineTypeColDefs[lineType]
	fmt.Println("coldef:\n:", coldef)

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

		// need to populate header fields
	}

	// now append data fields to doc
}
