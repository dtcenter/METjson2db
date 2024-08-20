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
			case string:
				fmt.Println(key, "\t", val, "\t", "string")
				doc.headerFields[key] = makeStringCbDataValue(val.(string))
			case uint64:
				fmt.Println(key, "\t", val, "\t", "unit64")
			case float64:
				fmt.Println(key, "\t", val, "\t", "float64")
				doc.headerFields[key] = makeFloatCbDataValue(val.(float64))
			default:
				fmt.Println("unknown type:", key, "\t", reflect.TypeOf(val), "\t", t)
			}
		}
	}

	data := parsed["data"].(map[string]any)
	dataKeys := maps.Keys(data)
	fmt.Println("data keys:\n", dataKeys)
	for i := 0; i < len(dataKeys); i++ {
		dataKey := dataKeys[i]
		doc.data[dataKey] = make(map[string]CbDataValue)
		dataVal := data[dataKey].(map[string]any)
		valKeys := maps.Keys(dataVal)
		fmt.Println("\tval keys:\n", valKeys)
		for i := 0; i < len(valKeys); i++ {
			key := valKeys[i]
			val := dataVal[key].(float64)
			doc.data[dataKey][key] = makeFloatCbDataValue(val)
			fmt.Println("\t", key, val)
		}
	}

	fmt.Println("data:\n", data)
	return doc, err
}
