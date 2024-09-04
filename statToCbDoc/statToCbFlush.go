package main

import (
	"encoding/json"
	"log"
	"os"

	"golang.org/x/exp/maps"
	// "github.com/couchbase/gocb/v2"
)

// init runs before main() is evaluated
func init() {
	log.Println("StatToCbFlush:init()")
}

func statToCbFlush(flushFinal bool) {
	log.Printf("statToCbFlush(FlushToDbDataSectionMaxCount:%d, outputFolder:%s)", conf.FlushToDbDataSectionMaxCount, conf.OutputFolder)

	/*
		See spec in readme, section:
		# Output location, configuration and logic
	*/
	conn := getDbConnection(credentials)
	for id, doc := range cbDocs {
		dataLen := int64(len(maps.Keys(doc.data)))
		if flushFinal || (dataLen >= conf.FlushToDbDataSectionMaxCount) {
			if conf.WriteJSONsToFile {
				flushToFiles(id)
			}
			if conf.UploadToDb {
				flushToDb(conn, id)
			}
		}
	}
}

func flushToFiles(id string) {
	log.Printf("flushToFiles(%s)", id)

	doc := cbDocs[id]
	log.Printf("data keys:%v", maps.Keys(doc.data))

	docStr := []byte(doc.toJSONString())
	fileName := conf.OutputFolder + "/" + id + ".json"
	/*
		Read if file exists and merge
	*/
	err := os.WriteFile(fileName, docStr, 0644)
	if err != nil {
		log.Printf("Error writing output:%s", fileName)
	}
}

func flushToDb(conn CbConnection, id string) {
	log.Printf("flushToDb(%s)", id)

	doc := cbDocs[id]
	log.Printf("data keys:%v", maps.Keys(doc.data))

	/*
		Read if file exists and merge if conf[overWriteData] == false
		otherwise ovewrite doc
	*/

	var anyJson map[string]interface{}
	json.Unmarshal([]byte(doc.toJSONString()), &anyJson)
	_, err := conn.Collection.Upsert(doc.headerFields["ID"].StringVal, anyJson, nil)
	if err != nil {
		log.Fatal(err)
	}

}
