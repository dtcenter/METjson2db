package utils

import (
	"encoding/json"
	"log"
	"os"
	"reflect"

	"github.com/relvacode/iso8601"
	"golang.org/x/exp/maps"

	// "github.com/couchbase/gocb/v2"

	"github.com/NOAA-GSL/METdatacb/pkg/types"
)

// init runs before main() is evaluated
func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("StatToCbUtils:init()")
}

func readCbDocument(file string) (types.CbDataDocument, error) {
	log.Println("CbDataDocument(" + file + ")")

	doc := types.CbDataDocument{}
	doc.Init()

	jsonText, err := os.ReadFile(file)
	if err != nil {
		log.Fatal("opening Cb doc file", err.Error())
		return doc, err
	}
	var parsed map[string]any
	err = json.Unmarshal(jsonText, &parsed)
	keys := maps.Keys(parsed)
	log.Printf("keys:\n%v", keys)

	for i := 0; i < len(keys); i++ {
		key := keys[i]
		val := parsed[key]
		if key != "data" {
			switch t := val.(type) {
			case []uint8:
				// t is []uint8
			case string:
				log.Printf(key, "\t", val, "\t", "string")
				doc.HeaderFields[key] = types.MakeStringCbDataValue(val.(string))
			case uint64:
				log.Printf(key, "\t", val, "\t", "unit64")
			case float64:
				log.Printf(key, "\t", val, "\t", "float64")
				doc.HeaderFields[key] = types.MakeFloatCbDataValue(val.(float64))
			default:
				log.Printf("unknown type:%s\tType:%v\tt:%v", key, reflect.TypeOf(val), t)
			}
		}
	}

	data := parsed["data"].(map[string]any)
	dataKeys := maps.Keys(data)
	// log.Printf("data keys:\n%v", dataKeys)
	for i := 0; i < len(dataKeys); i++ {
		dataKey := dataKeys[i]
		doc.Data[dataKey] = make(map[string]types.CbDataValue)
		dataVal := data[dataKey].(map[string]any)
		valKeys := maps.Keys(dataVal)
		log.Printf("\tval keys:\n%v", valKeys)
		for i := 0; i < len(valKeys); i++ {
			key := valKeys[i]
			val := dataVal[key].(float64)
			doc.Data[dataKey][key] = types.MakeFloatCbDataValue(val)
			log.Printf("\t%s,%f", key, val)
		}
	}

	log.Printf("data:\n%v", data)
	return doc, err
}

func StatDateToEpoh(dateStr string) int64 {
	// 20240203_120000 => 2024-02-03T12:00:00
	yyyy := dateStr[0:4]
	mm := dateStr[4:6]
	dd := dateStr[6:8]
	hh := dateStr[9:11]
	strISO8601 := yyyy + "-" + mm + "-" + dd + "T" + hh + ":00:00"
	// log.Printf("strISO8601:", strISO8601)
	t, _ := iso8601.ParseString(strISO8601)

	return int64(t.Unix())
}
