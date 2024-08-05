package main

import "fmt"

type StatToCbBuilder_SL1L2 struct {
	lineType string
	fields   []string
}

/*
// getHouse implements IStatToCbBuilder.
func (b *StatToCbBuilder_SL1L2) getHouse() {
	panic("unimplemented")
}
*/

func newStatToCbBuilder_SL1L2(fields []string) IStatToCbBuilder {
	return &StatToCbBuilder_SL1L2{lineType: "SL1L2", fields: fields}
}

func (b *StatToCbBuilder_SL1L2) processFields() bool {
	fmt.Println(string(b.lineType), ":processFields(", len(b.fields), ")")
	return true
}
