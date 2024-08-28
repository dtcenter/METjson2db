package main

import (
	"log"
	"os"

	"golang.org/x/exp/maps"
	// "github.com/couchbase/gocb/v2"
)

// init runs before main() is evaluated
func init() {
	log.Println("StatToCbFlush:init()")
}

func statToCbFlush() {
	log.Printf("statToCbFlush(FlushToDbDataSectionMaxCount:%d, outputFolder:%s)", conf.FlushToDbDataSectionMaxCount, conf.OutputFolder)

	/*
		See spec in readme, section:
		# Output location, configuration and logic
	*/
	for id, doc := range cbDocs {
		dataLen := int64(len(maps.Keys(doc.data)))
		if dataLen >= conf.FlushToDbDataSectionMaxCount {
			log.Printf("\tdoc-id:%s, data keys:%v", id, maps.Keys(doc.data))
			id := doc.headerFields["id"].StringVal
			docStr := []byte(doc.toJSONString())
			fileName := conf.OutputFolder + "/" + id + ".json"
			err := os.WriteFile(fileName, docStr, 0644)
			if err != nil {
				log.Printf("Error writing output:%s", fileName)
			}
		}
	}
}
