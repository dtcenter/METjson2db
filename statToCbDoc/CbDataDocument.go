package main

import (
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/exp/maps"
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

	shkeys := maps.Keys(doc.stringHeaderFields)
	for i := 0; i < len(shkeys); i++ {
		shf := shkeys[i]
		shv := doc.stringHeaderFields[shf]
		sb.WriteString("\t\"" + shf + "\": \"" + shv + "\",\n")
	}

	nhkeys := maps.Keys(doc.numericHeaderFields)
	for i := 0; i < len(nhkeys); i++ {
		nhf := nhkeys[i]
		nhv := doc.numericHeaderFields[nhf]
		fmt.Println(nhf, nhv)
		sb.WriteString("\t\"" + nhf + "\": " + fmt.Sprintf("%d", nhv) + ",\n")
	}

	ddkeys := maps.Keys(doc.data)
	if len(ddkeys) == 0 {
		sb.WriteString("}\n")
		return sb.String()
	}

	sb.WriteString("\t\"data\": {\n")

	for i := 0; i < len(ddkeys); i++ {
		dkey := ddkeys[i]
		dsec := doc.data[dkey]
		fmt.Println(dkey, dsec)

		sb.WriteString("\t\t\"" + dkey + "\": {\n")

		valkeys := maps.Keys(dsec)
		for i := 0; i < len(valkeys); i++ {
			valkey := valkeys[i]
			valval := dsec[valkey]
			valvals := strconv.FormatFloat(valval, 'f', -1, 64)
			// fmt.Println(ddkey, ddvals)

			if i == len(valkeys)-1 {
				sb.WriteString("\t\t\t\"" + valkey + "\": " + valvals + "\n")
				sb.WriteString("\t\t}\n")
			} else {
				sb.WriteString("\t\t\t\"" + valkey + "\": " + valvals + ",\n")
			}
		}
	}
	sb.WriteString("\t}\n}\n")
	return sb.String()
}
