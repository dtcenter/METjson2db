package types

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
	HeaderFields map[string]CbDataValue
	Data         map[string]DataSection
	Flushed      bool
	Mutex        *sync.RWMutex
}

func MakeStringCbDataValue(val string) CbDataValue {
	rv := CbDataValue{}
	rv.DataType = 0
	rv.StringVal = val
	return rv
}

func MakeIntCbDataValue(val int64) CbDataValue {
	rv := CbDataValue{}
	rv.DataType = 1
	rv.IntVal = val
	return rv
}

func MakeFloatCbDataValue(val float64) CbDataValue {
	rv := CbDataValue{}
	rv.DataType = 2
	rv.FloatVal = val
	return rv
}

func (doc *CbDataDocument) Init() {
	doc.HeaderFields = make(map[string]CbDataValue)
	doc.Data = make(map[string]DataSection)
	doc.Mutex = &sync.RWMutex{}
	doc.Flushed = false
}

func (doc *CbDataDocument) InitReturn(count int64, errors int64) {
	doc.HeaderFields = make(map[string]CbDataValue)
	doc.Data = make(map[string]DataSection)
	doc.HeaderFields["count"] = MakeIntCbDataValue(count)
	doc.HeaderFields["errors"] = MakeIntCbDataValue(errors)
}

func (doc *CbDataDocument) ToJSONString() string {
	var sb strings.Builder
	sb.WriteString("{\n")

	shkeys := maps.Keys(doc.HeaderFields)
	for i := 0; i < len(shkeys); i++ {
		shf := shkeys[i]
		shv := doc.HeaderFields[shf]
		switch {
		case shv.DataType == 0:
			sb.WriteString("\t\"" + shf + "\": \"" + shv.StringVal + "\",\n")
		case shv.DataType == 1:
			sb.WriteString("\t\"" + shf + "\": " + fmt.Sprintf("%d", shv.IntVal) + ",\n")
		case shv.DataType == 2:
			sb.WriteString("\t\"" + shf + "\": " + strconv.FormatFloat(shv.FloatVal, 'f', -1, 64) + ",\n")
		}
	}

	ddkeys := maps.Keys(doc.Data)
	if len(ddkeys) == 0 {
		sb.WriteString("}\n")
		return sb.String()
	}

	sb.WriteString("\t\"data\": {\n")

	for di := 0; di < len(ddkeys); di++ {
		dkey := ddkeys[di]
		dsec := doc.Data[dkey]

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
