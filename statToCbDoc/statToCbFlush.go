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
	// log.Printf("statToCbFlush(FlushToDbDataSectionMaxCount:%d, outputFolder:%s)", conf.FlushToDbDataSectionMaxCount, conf.OutputFolder)

	/*
		See spec in readme, section:
		# Output location, configuration and logic
	*/
	if conf.RunNonThreaded {
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
	} else {
		// distribute docs to channels, round-robin, for async processing
		idxFiles := 0
		idxDb := 0
		// for id, doc
		for _, doc := range cbDocs {
			dataLen := int64(len(maps.Keys(doc.data)))
			if flushFinal || (dataLen >= conf.FlushToDbDataSectionMaxCount) {
				if conf.WriteJSONsToFile {
					asynFlushToFileChannels[idxFiles] <- doc
					idxFiles++
					if idxFiles >= int(conf.ThreadsWriteToDisk) {
						idxFiles = 0
					}
				}
				if conf.UploadToDb {
					asynFlushToDbChannels[idxDb] <- doc
					idxDb++
					if idxDb >= int(conf.ThreadsDbUpload) {
						idxDb = 0
					}
				}
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
		TODO:
		1. Merge if conf[overWriteData] == false otherwise ovewrite doc
		2. Make flushToDb threaded and async
	*/

	var anyJson map[string]interface{}
	json.Unmarshal([]byte(doc.toJSONString()), &anyJson)

	// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
	_, err := conn.Collection.Upsert(doc.headerFields["ID"].StringVal, anyJson, nil)
	if err != nil {
		log.Fatal(err)
	}
}
