package core

import (

	//	"fmt"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	// "github.com/NOAA-GSL/MET-parser/pkg/structColumnDefs"
	// "github.com/NOAA-GSL/MET-parser/pkg/structColumnTypes"

	"github.com/NOAA-GSL/METdatacb/pkg/structColumnDefs"
	"github.com/NOAA-GSL/METdatacb/pkg/structColumnTypes"
)

// init runs before main() is evaluated
func init() {
	log.Println("statToJSON:init()")
}

// dummy function to satisfy the function signature of getExternalDocForId
func getMissingExternalDocForId(id string) (map[string]interface{}, error) {
	// fmt.Println("getExternalDocForId called with id:", id)
	// Put your own code here in this method but always return this exact error if the document is not found
	return nil, fmt.Errorf("%s: %s", structColumnTypes.DOC_NOT_FOUND, id)
}

func statFileToCbDocMetParser(filepath string) (map[string]interface{}, error) {
	log.Println("statFileToCbDocMetParser(" + filepath + ")")
	var doc map[string]interface{}
	var err error

	file, err := os.Open(filepath) // open the file
	if err != nil {
		log.Fatal("error opening file", err)
	}
	defer file.Close()
	rawData, err := io.ReadAll(file)
	if err != nil {
		log.Fatal("error reading file", err)
	}
	lines := strings.Split(string(rawData), "\n")
	headerLine := lines[0]
	for line := range lines {
		if line == 0 || lines[line] == "" {
			continue
		}
		dataLine := lines[line]
		doc, err = structColumnDefs.ParseLine(headerLine, dataLine, &doc, filepath, getMissingExternalDocForId)
		if err != nil {
			log.Fatalf("Expected no error, got %v", err)

		}
	}
	if doc == nil {
		log.Fatalf("Expected parsed document, got nil")
	}

	return doc, err
}
