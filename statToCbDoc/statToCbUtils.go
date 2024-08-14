package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"

	"golang.org/x/exp/maps"
	// "github.com/couchbase/gocb/v2"
)

// init runs before main() is evaluated
func init() {
	log.Println("StatToCbUtils:init()")
}

func createTestcbDocument() CbDataDocument {
	doc := CbDataDocument{}
	doc.init()
	doc.stringHeaderFields["id"] = "V11.1.0:GFS:SL1L2:TMP:FULL:P1000:P1000:1706986800"
	return doc
}

func readCbDocument(file string) (CbDataDocument, error) {
	log.Println("CbDataDocument(" + file + ")")

	doc := CbDataDocument{}
	doc.init()

	jsonText, err := os.ReadFile(file)
	if err != nil {
		log.Fatal("opening Cb doc file", err.Error())
		return doc, err
	}
	var parsed map[string]any
	err = json.Unmarshal(jsonText, &parsed)
	keys := maps.Keys(parsed)
	fmt.Println("keys:\n", keys)

	for i := 0; i < len(keys); i++ {
		key := keys[i]
		val := parsed[key]
		if key != "data" {
			switch t := val.(type) {
			case []uint8:
				// t is []uint8
			case uint64:
				fmt.Println(key, "\t", val, "\t", "unit64")
			case float64:
				fmt.Println(key, "\t", val, "\t", "float64")
				doc.numericHeaderFields[key] = int(val.(float64))
			case string:
				fmt.Println(key, "\t", val, "\t", "string")
				doc.stringHeaderFields[key] = val.(string)
			default:
				fmt.Println("unknown type:", key, "\t", reflect.TypeOf(val), "\t", t)
			}
		}
	}

	data := parsed["data"].(map[string]any)
	dataKeys := maps.Keys(data)
	fmt.Println("data keys:\n", dataKeys)
	for i := 0; i < len(dataKeys); i++ {
		key := dataKeys[i]
		val := data[key]
		if key != "data" {
			switch t := val.(type) {
			case []uint8:
				// t is []uint8
			case uint64:
				fmt.Println(key, "\t", val, "\t", "unit64")
			case float64:
				fmt.Println(key, "\t", val, "\t", "float64")
				doc.numericHeaderFields[key] = int(val.(float64))
			case string:
				fmt.Println(key, "\t", val, "\t", "string")
				doc.stringHeaderFields[key] = val.(string)
			default:
				fmt.Println("unknown type:", key, "\t", reflect.TypeOf(val), "\t", t)
			}
		}
	}

	fmt.Println("data:\n", data)
	return doc, err
}
