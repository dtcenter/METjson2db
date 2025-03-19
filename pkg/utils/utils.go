package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
)

// init runs before main() is evaluated
func init() {
	slog.Debug("utils:init()")
}

func getTabbedString(count int) (rv string) {
	rv = ""
	for i := 0; i < count; i++ {
		rv = rv + "\t"
	}
	return rv
}

func printStringArray(in []string) {
	for i := 0; i < len(in); i++ {
		fmt.Println(in[i])
	}
}

func DocPrettyPrint(in map[string]interface{}) string {
	jsonText, err := json.Marshal(in)
	if err != nil {
		fmt.Println("ERROR PROCESSING STREAMING OUTPUT:", err)
	}
	var out bytes.Buffer
	json.Indent(&out, jsonText, "", "\t")
	return out.String()
}

func JsonPrettyPrint(in []interface{}) string {
	jsonText, err := json.Marshal(in)
	if err != nil {
		fmt.Println("ERROR PROCESSING STREAMING OUTPUT:", err)
	}
	var out bytes.Buffer
	json.Indent(&out, jsonText, "", "\t")
	return out.String()
}

func JsonPrettyPrintStruct(in interface{}) string {
	jsonText, err := json.Marshal(in)
	if err != nil {
		fmt.Println("ERROR PROCESSING STREAMING OUTPUT:", err)
	}
	var out bytes.Buffer
	json.Indent(&out, jsonText, "", "\t")
	return out.String()
}

func walkJsonMap(val map[string]interface{}, depth int) {
	for k, v := range val {
		switch vv := v.(type) {
		case string:
			fmt.Println(getTabbedString(depth), k, ":", vv, " (string)")
		case float64:
			fmt.Println(getTabbedString(depth), k, ":", vv, " (float64)")
		case []interface{}:
			fmt.Println(getTabbedString(depth), k, ":", " (array)")
			for i, u := range vv {
				fmt.Println(getTabbedString(depth+1), i, u)
			}
		case map[string]interface{}:
			fmt.Println(getTabbedString(depth), k, ":", " (map)")
			m := v.(map[string]interface{})
			walkJsonMap(m, depth+1)
		default:
			fmt.Println(getTabbedString(depth), k, vv, " (unknown)")
		}
	}
}

func ConvertSlice[E any](in []any) (out []E) {
	out = make([]E, 0, len(in))
	for _, v := range in {
		out = append(out, v.(E))
	}
	return
}

func PrettyPrint(i interface{}) string {
	s, _ := json.MarshalIndent(i, "", "\t")
	fmt.Println(string(s))
	return string(s)
}

/*
GUID length - 36
00b9bde7-7abd-451b-96f1-43d33a274eca
deba6ae5-30d6-4876-8796-6685352995f4
*/
func GetGUID() string {
	id := uuid.New()
	return id.String()
}
