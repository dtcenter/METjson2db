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

func statFileToCbDoc(filepath string) (bool, error) {
	log.Println("statFileToCbDoc(" + filepath + ")")

	rv := false

	inputFile, err := os.Open(filepath)
	if err != nil {
		log.Fatal("error opening file\n", err.Error())
		inputFile.Close()
		return rv, err
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

		/*
			_, ok := builders[lineType]
			if false == ok {
				builders[lineType] = getBuilder(lineType, fields)
			}
			builder, ok := builders[lineType]
		*/
		builder := getBuilder(lineType, fields)
		if nil != builder {
			builder.processFields()
		} else {
			fmt.Println("Unknown line tye:", lineType)
		}

	}
	fmt.Println("lineCount:", lineCount)

	return rv, err
}
