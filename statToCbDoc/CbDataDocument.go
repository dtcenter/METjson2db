package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/exp/maps"
	// "github.com/couchbase/gocb/v2"
)

type CbDataValue struct {
	DataType  int // 0-string, 1-float64, 2-int64
	StringVal string
	IntVal    int64
	FloatVal  float64
}

type DataSection map[string]CbDataValue

type CbDataDocument struct {
	headerFields map[string]CbDataValue
	data         map[string]DataSection
	flushed      bool
	mutex        *sync.RWMutex
}

func makeStringCbDataValue(val string) CbDataValue {
	rv := CbDataValue{}
	rv.DataType = 0
	rv.StringVal = val
	return rv
}

func makeIntCbDataValue(val int64) CbDataValue {
	rv := CbDataValue{}
	rv.DataType = 1
	rv.IntVal = val
	return rv
}

func makeFloatCbDataValue(val float64) CbDataValue {
	rv := CbDataValue{}
	rv.DataType = 2
	rv.FloatVal = val
	return rv
}

func (doc *CbDataDocument) init() {
	doc.headerFields = make(map[string]CbDataValue)
	doc.data = make(map[string]DataSection)
	doc.mutex = &sync.RWMutex{}
	doc.flushed = false
}

func (doc *CbDataDocument) initReturn(count int64, errors int64) {
	doc.headerFields = make(map[string]CbDataValue)
	doc.data = make(map[string]DataSection)
	doc.headerFields["count"] = makeIntCbDataValue(count)
	doc.headerFields["errors"] = makeIntCbDataValue(errors)
}

func (doc *CbDataDocument) toJSONString() string {
	var sb strings.Builder
	sb.WriteString("{\n")

	shkeys := maps.Keys(doc.headerFields)
	for i := 0; i < len(shkeys); i++ {
		shf := shkeys[i]
		shv := doc.headerFields[shf]
		switch {
		case shv.DataType == 0:
			sb.WriteString("\t\"" + shf + "\": \"" + shv.StringVal + "\",\n")
		case shv.DataType == 1:
			sb.WriteString("\t\"" + shf + "\": " + fmt.Sprintf("%d", shv.IntVal) + ",\n")
		case shv.DataType == 2:
			sb.WriteString("\t\"" + shf + "\": " + strconv.FormatFloat(shv.FloatVal, 'f', -1, 64) + ",\n")
		}
	}

	ddkeys := maps.Keys(doc.data)
	if len(ddkeys) == 0 {
		sb.WriteString("}\n")
		return sb.String()
	}

	sb.WriteString("\t\"data\": {\n")

	for di := 0; di < len(ddkeys); di++ {
		dkey := ddkeys[di]
		dsec := doc.data[dkey]

		sb.WriteString("\t\t\"" + dkey + "\": {\n")

		valkeys := maps.Keys(dsec)
		for i := 0; i < len(valkeys); i++ {
			valkey := valkeys[i]
			valval := dsec[valkey]

			valvals := ""

			switch {
			case valval.DataType == 0:
				valvals = valval.StringVal
			case valval.DataType == 1:
				valvals_0 := fmt.Sprintf("%d", valval.IntVal)
				valvals = strings.TrimLeft(valvals_0, "0")
			case valval.DataType == 2:
				valvals = strconv.FormatFloat(valval.FloatVal, 'f', -1, 64)
			}

			if i == len(valkeys)-1 {
				sb.WriteString("\t\t\t\"" + valkey + "\": " + valvals + "\n")
				sb.WriteString("\t\t}")
			} else {
				sb.WriteString("\t\t\t\"" + valkey + "\": " + valvals + ",\n")
			}
		}
		if di < len(ddkeys)-1 {
			sb.WriteString(",\n")
		}
	}

	sb.WriteString("\n\t}\n}\n")
	return sb.String()
}
