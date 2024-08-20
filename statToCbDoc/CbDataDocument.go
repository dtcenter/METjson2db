package main

import (
	"fmt"
	"strconv"
	"strings"

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

	for i := 0; i < len(ddkeys); i++ {
		dkey := ddkeys[i]
		dsec := doc.data[dkey]
		fmt.Println(dkey, dsec)

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
				valvals = fmt.Sprintf("%d", valval.IntVal)
			case valval.DataType == 2:
				valvals = strconv.FormatFloat(valval.FloatVal, 'f', -1, 64)
			}

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
