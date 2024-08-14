package main

import (
	"fmt"
	"strings"
	// "github.com/couchbase/gocb/v2"
)

type DataSection map[string]float64

type CbDataDocument struct {
	stringHeaderFields  map[string]string
	numericHeaderFields map[string]int
	data                map[string]DataSection
}

func (doc *CbDataDocument) init() {
	doc.stringHeaderFields = make(map[string]string)
	doc.numericHeaderFields = make(map[string]int)
	doc.data = make(map[string]DataSection)
}

func (doc *CbDataDocument) toJSONString() string {
	var sb strings.Builder
	sb.WriteString("{\n")

	for shf, shv := range doc.stringHeaderFields {
		fmt.Println(shf, shv)
		sb.WriteString("\t\"" + shf + "\": \"" + shv + "\",\n")
	}
	for nhf, nhv := range doc.numericHeaderFields {
		fmt.Println(nhf, nhv)
		sb.WriteString("\t" + nhf + ": " + fmt.Sprintf("%d", nhv) + ",\n")
	}
	for dkey, dsec := range doc.data {
		fmt.Println(dkey, dsec)
		sb.WriteString("\tdata: {\n")

		for ddkey, ddoc := range dsec {
			fmt.Println(ddkey, ddoc)
			sb.WriteString("\t\t\"" + ddkey + "\" {\n")
		}
		sb.WriteString("\t}\n")
	}

	sb.WriteString("}\n")
	return sb.String()
}
